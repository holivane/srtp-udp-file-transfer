import argparse
import functools
import os
import random
import socket
import struct
import sys
import time


print = functools.partial(print, flush=True)

HEADER_FORMAT = "!IIB"
ACK_FORMAT = "!I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
MAX_PAYLOAD = 1024
MAX_PACKET_SIZE = HEADER_SIZE + MAX_PAYLOAD
FLAG_FINAL = 1


def enviar_ack(sock, endereco, seq_num):
    ack = struct.pack(ACK_FORMAT, seq_num)
    sock.sendto(ack, endereco)
    print(f"[SERVIDOR] ACK enviado para seq={seq_num}")


def remover_parcial(caminho_saida, arquivo_aberto):
    if arquivo_aberto is not None and not arquivo_aberto.closed:
        arquivo_aberto.close()

    # Rollback: se a transacao falhar, o arquivo incompleto nao deve permanecer no disco.
    if os.path.exists(caminho_saida):
        os.remove(caminho_saida)
        print(f"[SERVIDOR] Transferencia abortada. Arquivo parcial removido: {caminho_saida}")
    else:
        print("[SERVIDOR] Transferencia abortada. Nenhum arquivo parcial encontrado")


def executar_servidor(args):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.porta))
    sock.settimeout(0.5)

    print(f"[SERVIDOR] Aguardando pacotes UDP em {args.host}:{args.porta}")
    print(f"[SERVIDOR] Saida: {args.saida} | perda simulada: {args.perda_dados:.0%}")

    arquivo_saida = None
    ativo = False
    trans_id_atual = None
    esperado = 0
    ultimo_pacote = None
    ultima_transacao_concluida = None

    try:
        while True:
            if ativo and ultimo_pacote is not None:
                tempo_ocioso = time.perf_counter() - ultimo_pacote
                if tempo_ocioso > args.timeout_transacao:
                    remover_parcial(args.saida, arquivo_saida)
                    arquivo_saida = None
                    ativo = False
                    trans_id_atual = None
                    esperado = 0
                    ultimo_pacote = None

            try:
                pacote, endereco = sock.recvfrom(MAX_PACKET_SIZE)
            except socket.timeout:
                continue

            # Simula perda de pacote de dados no servidor: o pacote some e nenhum ACK e enviado.
            if random.random() < args.perda_dados:
                print("[SERVIDOR] Pacote descartado pela simulacao de perda")
                continue

            if len(pacote) < HEADER_SIZE:
                print("[SERVIDOR] Pacote invalido: menor que o cabecalho")
                continue

            seq_num, trans_id, flag = struct.unpack(HEADER_FORMAT, pacote[:HEADER_SIZE])
            payload = pacote[HEADER_SIZE:]

            if not ativo and ultima_transacao_concluida == (trans_id, seq_num):
                print(f"[SERVIDOR] Reenvio do ultimo pacote final seq={seq_num}; reenviando ACK")
                enviar_ack(sock, endereco, seq_num)
                continue

            if not ativo:
                if seq_num != 0:
                    print(f"[SERVIDOR] Pacote inicial invalido seq={seq_num}; aguardado seq=0")
                    continue

                arquivo_saida = open(args.saida, "wb")
                ativo = True
                trans_id_atual = trans_id
                esperado = 0
                ultimo_pacote = time.perf_counter()
                print(f"[SERVIDOR] Nova transferencia iniciada. Trans ID={trans_id}")

            if trans_id != trans_id_atual:
                print(f"[SERVIDOR] Trans ID {trans_id} ignorado; atual={trans_id_atual}")
                continue

            ultimo_pacote = time.perf_counter()

            if seq_num == esperado:
                arquivo_saida.write(payload)
                arquivo_saida.flush()
                print(f"[SERVIDOR] Seq={seq_num} gravado ({len(payload)} bytes)")
                enviar_ack(sock, endereco, seq_num)

                if flag == FLAG_FINAL:
                    arquivo_saida.close()
                    arquivo_saida = None
                    ativo = False
                    trans_id_atual = None
                    esperado = 0
                    ultimo_pacote = None
                    ultima_transacao_concluida = (trans_id, seq_num)
                    print(f"[SERVIDOR] Transferencia concluida com sucesso: {args.saida}")
                else:
                    esperado += 1
            elif seq_num < esperado:
                # Pacote duplicado: o servidor confirma novamente, mas nao grava duas vezes.
                print(f"[SERVIDOR] Seq={seq_num} duplicado; reenviando ACK sem regravar")
                enviar_ack(sock, endereco, seq_num)
            else:
                print(f"[SERVIDOR] Seq fora de ordem: recebido={seq_num}, esperado={esperado}")
                if esperado > 0:
                    enviar_ack(sock, endereco, esperado - 1)
    except KeyboardInterrupt:
        print("\n[SERVIDOR] Encerrado manualmente")
        if ativo:
            remover_parcial(args.saida, arquivo_saida)
        return 130
    finally:
        sock.close()

    return 0


def criar_parser():
    parser = argparse.ArgumentParser(
        description="Servidor SRTP: recebe arquivo com confiabilidade sobre UDP."
    )
    parser.add_argument("--host", default="127.0.0.1", help="IP local do servidor")
    parser.add_argument("--porta", type=int, default=9000, help="Porta UDP")
    parser.add_argument("--saida", default="recebido_arquivo.bin", help="Arquivo de saida")
    parser.add_argument("--perda-dados", type=float, default=0.30, help="Probabilidade de perda simulada de dados")
    parser.add_argument("--timeout-transacao", type=float, default=10.0, help="Tempo para rollback do arquivo parcial")
    return parser


if __name__ == "__main__":
    sys.exit(executar_servidor(criar_parser().parse_args()))
