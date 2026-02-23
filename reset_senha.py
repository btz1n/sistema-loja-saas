import sqlite3
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

DB = "loja.db"

store_name = "imperio"
username = "eumrm"
new_password = "123456"

conn = sqlite3.connect(DB)
cur = conn.cursor()

store = cur.execute("SELECT id FROM stores WHERE name=?", (store_name,)).fetchone()
if not store:
    raise SystemExit("Loja não encontrada")

store_id = store[0]
user = cur.execute("SELECT id FROM users WHERE store_id=? AND username=?", (store_id, username)).fetchone()
if not user:
    raise SystemExit("Usuário não encontrado")

new_hash = pwd_context.hash(new_password)
cur.execute("UPDATE users SET password_hash=? WHERE id=?", (new_hash, user[0]))
conn.commit()
conn.close()

print("Senha resetada com sucesso!")
print("Loja:", store_name)
print("Usuário:", username)
print("Nova senha:", new_password)