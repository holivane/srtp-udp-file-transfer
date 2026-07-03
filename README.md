# Estudo Guiado 01 - SRTP sobre UDP

Entrega da pratica "Criacao de Protocolos: Transferencia de Arquivos Confiavel sobre UDP".

SRTP significa Simple Reliable Transfer Protocol: um protocolo proprio, definido para esta atividade, que garante entrega confiavel de arquivos sobre UDP por meio de numero de sequencia, ACK, timeout e retransmissao.

## Arquivos

- `cliente.py`: emissor do arquivo usando UDP com Stop-and-Wait.
- `servidor.py`: receptor com ACK, controle de duplicados e rollback.
- `respostas_ava.txt`: respostas teoricas para envio no AVA.
- `roteiro_video.txt`: roteiro objetivo para gravar o video de entrega.

## Como executar

No primeiro terminal:

```bash
python servidor.py --saida recebido_foto.jpg --perda-dados 0.30
```

No segundo terminal:

```bash
python cliente.py foto.jpg --perda-ack 0.30
```

Para testar sem perda simulada:

```bash
python servidor.py --saida recebido_foto.jpg --perda-dados 0.00
python cliente.py foto.jpg --perda-ack 0.00
```

## Teste de rollback

1. Inicie o servidor:

```bash
python servidor.py --saida recebido_incompleto.jpg --perda-dados 0.00
```

2. Inicie o cliente com um arquivo grande:

```bash
python cliente.py foto_grande.jpg --perda-ack 0.00
```

3. Feche o cliente com `Ctrl+C` durante a transferencia.
4. Aguarde 10 segundos no servidor.
5. O servidor deve remover o arquivo parcial automaticamente.
