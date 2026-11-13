#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
remote_client.py — 远程控制联调客户端
向网关发一个 REQUEST（service=0xF001 串口桥），载荷是一个 AA..55 串口帧，
验证：网关 → 上位机(串口桥) → 设备 → 上位机回 ACK 的整条链路。

用法（在树莓派上）：
    python3 remote_client.py                # 连 127.0.0.1:9090
    python3 remote_client.py 192.168.31.98 9090
"""
import socket, struct, sys

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 9090

MAGIC        = 0xABCDEF01
SVC_SERIAL   = 0x0000F001
MSG_REQUEST  = 0x00
MSG_RESPONSE = 0x02


def make_frame(msg_type, service_id, method_id, client_id, payload=b""):
    """25 字节大端头 + payload"""
    return struct.pack(
        ">IIIII5B",
        MAGIC, service_id, method_id, len(payload), client_id,
        msg_type, 0, 0, 0, 0,
    ) + payload


def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("连接被关闭")
        buf += chunk
    return buf


# 构造一个 AA..55 舵机角度帧：AA 04 02 01 5A 校验 55（CMD_SERVO_ANGLE, DEVICE_SERVO, 角度 0x5A=90°）
cmd, dev, data = 0x04, 0x02, bytes([0x5A])
checksum = (cmd + dev + len(data) + sum(data)) & 0xFF
serial_frame = bytes([0xAA, cmd, dev, len(data)]) + data + bytes([checksum, 0x55])
print("串口帧:", serial_frame.hex(" ").upper())

s = socket.create_connection((HOST, PORT), timeout=5)
client_id = 2001
s.sendall(make_frame(MSG_REQUEST, SVC_SERIAL, 0x01, client_id, serial_frame))
print(f"已发送 REQUEST -> {HOST}:{PORT} service=0x{SVC_SERIAL:08X}")

hdr = recv_exact(s, 25)
magic, svc, method, length, client, mtype, err, ver, r0, r1 = struct.unpack(">IIIII5B", hdr)
body = recv_exact(s, length) if length else b""
print(f"收到 RESPONSE: magic=0x{magic:08X} service=0x{svc:08X} type=0x{mtype:02X} "
      f"error={err} body={body.hex(' ').upper()}")

if mtype == MSG_RESPONSE and err == 0:
    print("✅ 远程控制链路通：网关 → 上位机(串口桥) → 设备，已回 ACK")
else:
    print("❌ 未收到正常 RESPONSE（检查上位机是否已连网关、是否添加并连接了设备）")

s.close()
