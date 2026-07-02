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
MAX_PAYLOAD = 1024
FLAG_DADOS = 0
FLAG_FINAL = 1


def montar_pacote(seq_num, trans_id, flag, payload):
    cabecalho = struct.pack(HEADER_FORMAT, seq_num, trans_id, flag)
    return cabecalho + payload


def receber_ack(sock, perda_ack):
    dados, _ = sock.recvfrom(struct.calcsize(ACK_FORMAT))

    # Simula a perda do ACK no lado do cliente para forcar timeout/retransmissao.
    if random.random() < perda_ack:
        raise socket.timeout("ACK descartado pela simulacao de perda")

    return struct.unpack(ACK_FORMAT, dados)[0]


def enviar_com_retry(sock, endereco, pacote, seq_num, timeout, max_tentativas, perda_ack):
    for tentativa in range(1, max_tentativas + 1):
        sock.sendto(pacote, endereco)
        print(f"[CLIENTE] Pacote seq={seq_num} enviado (tentativa {tentativa}/{max_tentativas})")

        try:
            ack_num = receber_ack(sock, perda_ack)
            if ack_num == seq_num:
                print(f"[CLIENTE] ACK recebido para seq={ack_num}")
                return True

            print(f"[CLIENTE] ACK inesperado ({ack_num}); aguardado seq={seq_num}")
        except socket.timeout:
            print(f"[CLIENTE] Timeout aguardando ACK do seq={seq_num}")

        if tentativa < max_tentativas:
            print(f"[CLIENTE] Retransmissao do seq={seq_num}")

    return False


def transferir_arquivo(args):
    if not os.path.isfile(args.arquivo):
        print(f"[ERRO] Arquivo nao encontrado: {args.arquivo}")
        return 1

    tamanho_arquivo = os.path.getsize(args.arquivo)
    trans_id = args.trans_id if args.trans_id is not None else random.randint(1, 0xFFFFFFFF)
    endereco = (args.host, args.porta)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(args.timeout)

    print(f"[CLIENTE] Enviando '{args.arquivo}' para {args.host}:{args.porta}")
    print(f"[CLIENTE] Trans ID={trans_id} | tamanho={tamanho_arquivo} bytes")

    seq_num = 0
    bytes_confirmados = 0
    inicio = time.perf_counter()

    try:
        with open(args.arquivo, "rb") as arquivo:
            if tamanho_arquivo == 0:
                pacote = montar_pacote(seq_num, trans_id, FLAG_FINAL, b"")
                if not enviar_com_retry(sock, endereco, pacote, seq_num, args.timeout, args.tentativas, args.perda_ack):
                    print("[CLIENTE] Transferencia abortada: limite de tentativas excedido")
                    return 2
            else:
                while True:
                    payload = arquivo.read(MAX_PAYLOAD)
                    if not payload:
                        break

                    flag = FLAG_FINAL if arquivo.tell() == tamanho_arquivo else FLAG_DADOS
                    pacote = montar_pacote(seq_num, trans_id, flag, payload)

                    if not enviar_com_retry(sock, endereco, pacote, seq_num, args.timeout, args.tentativas, args.perda_ack):
                        print("[CLIENTE] Transferencia abortada: limite de tentativas excedido")
                        return 2

                    bytes_confirmados += len(payload)
                    seq_num += 1

                    if flag == FLAG_FINAL:
                        break
    except KeyboardInterrupt:
        print("\n[CLIENTE] Encerrado manualmente no meio da transferencia")
        return 130
    finally:
        sock.close()

    fim = time.perf_counter()
    tempo = max(fim - inicio, 0.000001)
    taxa_mbps = (bytes_confirmados * 8) / tempo / 1_000_000

    print("[CLIENTE] Transferencia concluida com sucesso")
    print(f"[CLIENTE] Tempo: {tempo:.4f}s | Taxa efetiva: {taxa_mbps:.4f} Mbps")
    return 0


def criar_parser():
    parser = argparse.ArgumentParser(
        description="Cliente SRTP: transferencia confiavel de arquivo sobre UDP."
    )
    parser.add_argument("arquivo", help="Arquivo que sera enviado")
    parser.add_argument("--host", default="127.0.0.1", help="IP do servidor")
    parser.add_argument("--porta", type=int, default=9000, help="Porta UDP do servidor")
    parser.add_argument("--timeout", type=float, default=1.0, help="Tempo maximo aguardando ACK")
    parser.add_argument("--tentativas", type=int, default=5, help="Tentativas por pacote")
    parser.add_argument("--perda-ack", type=float, default=0.30, help="Probabilidade de perda simulada de ACK")
    parser.add_argument("--trans-id", type=int, default=None, help="ID fixo opcional da transferencia")
    return parser


if __name__ == "__main__":
    sys.exit(transferir_arquivo(criar_parser().parse_args()))
