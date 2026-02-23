from datetime import datetime, date
from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime, ForeignKey
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Session
import secrets
SECRET = secrets.token_hex(32)
from sqlalchemy import func
from fastapi.responses import PlainTextResponse
# ======================
# CONFIG
# ======================
import os
from sqlalchemy import create_engine

DATABASE_URL = os.environ.get("DATABASE_URL")

# Se não tiver DATABASE_URL (rodando local), usa SQLite
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./loja.db"
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    # Render às vezes usa postgres://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

app = FastAPI()
BASE_DIR = Path(__file__).resolve().parent

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
# ======================
# MODELS
# ======================
class Store(Base):
    __tablename__ = "stores"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    users = relationship("User", back_populates="store")


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    username = Column(String, nullable=False)
    password_hash = Column(String, nullable=False)
    role = Column(String, default="admin")  # admin / staff

    store = relationship("Store", back_populates="users")


class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, nullable=False, index=True)
    name = Column(String, nullable=False)
    sku = Column(String, default="")
    price = Column(Float, default=0.0)
    stock = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)


class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, nullable=False, index=True)
    name = Column(String, nullable=False)
    phone = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)


class Sale(Base):
    __tablename__ = "sales"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, nullable=False, index=True)
    customer_name = Column(String, default="Cliente avulso")
    payment_method = Column(String, default="dinheiro")
    total = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)

    items = relationship("SaleItem", back_populates="sale", cascade="all, delete-orphan")


class SaleItem(Base):
    __tablename__ = "sale_items"
    id = Column(Integer, primary_key=True)
    sale_id = Column(Integer, ForeignKey("sales.id"), nullable=False)
    product_id = Column(Integer, nullable=False)
    product_name = Column(String, nullable=False)
    qty = Column(Integer, default=1)
    unit_price = Column(Float, default=0.0)
    line_total = Column(Float, default=0.0)

    sale = relationship("Sale", back_populates="items")

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, nullable=False, index=True)

    customer_name = Column(String, default="Cliente avulso")
    customer_phone = Column(String, default="")
    address = Column(String, default="")
    notes = Column(String, default="")  # observação do pedido

    payment_method = Column(String, default="dinheiro")  # dinheiro/pix/cartao
    delivery_fee = Column(Float, default=0.0)

    status = Column(String, default="novo")  # novo/separando/saiu/entregue/cancelado
    total = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)

    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)

    product_id = Column(Integer, nullable=False)
    product_name = Column(String, nullable=False)
    qty = Column(Integer, default=1)
    unit_price = Column(Float, default=0.0)
    line_total = Column(Float, default=0.0)

    order = relationship("Order", back_populates="items")
# ======================
# DB INIT + SEED
# ======================
Base.metadata.create_all(bind=engine)

def seed_if_empty():
    db = SessionLocal()
    try:
        store = db.query(Store).first()
        if not store:
            store = Store(name="Loja Demo")
            db.add(store)
            db.commit()
            db.refresh(store)

            admin = User(
                store_id=store.id,
                username="admin",
                password_hash=pwd_context.hash("admin123"),
                role="admin"
            )
            db.add(admin)

            db.add_all([
                Product(store_id=store.id, name="Camiseta Preta", sku="CAM-001", price=59.90, stock=20),
                Product(store_id=store.id, name="Boné", sku="BONE-001", price=39.90, stock=10),
            ])
            db.add_all([
                Customer(store_id=store.id, name="Cliente Exemplo", phone="(00) 00000-0000")
            ])

            db.commit()
    finally:
        db.close()

seed_if_empty()

# ======================
# HELPERS
# ======================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Sessão simples via cookie: store_id e user_id
def require_auth(request: Request, db: Session = Depends(get_db)):
    user_id = request.cookies.get("user_id")
    store_id = request.cookies.get("store_id")
    if not user_id or not store_id:
        raise HTTPException(status_code=401, detail="Não autenticado.")
    user = db.query(User).filter(User.id == int(user_id), User.store_id == int(store_id)).first()
    if not user:
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    return user

def today_range():
    start = datetime.combine(date.today(), datetime.min.time())
    end = datetime.combine(date.today(), datetime.max.time())
    return start, end

# ======================
# AUTH
# ======================
@app.get("/admin/create_user", response_class=HTMLResponse)
def admin_create_user_page(request: Request):
    return templates.TemplateResponse("create_user.html", {"request": request})


@app.post("/admin/create_user")
def admin_create_user(
    store_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    store_name = store_name.strip()
    username = username.strip()

    store = db.query(Store).filter(func.lower(Store.name) == store_name.lower()).first()
    if not store:
        raise HTTPException(400, "Loja não encontrada. Confira o nome.")

    existing = (
        db.query(User)
        .filter(User.store_id == store.id, func.lower(User.username) == username.lower())
        .first()
    )
    if existing:
        raise HTTPException(400, "Usuário já existe nessa loja.")

    u = User(
        store_id=store.id,
        username=username,
        role="admin",
        password_hash=pwd_context.hash(password.strip()),
    )
    db.add(u)
    db.commit()

    return RedirectResponse("/login", status_code=302)

@app.get("/admin/reset_password", response_class=HTMLResponse)
def reset_password_page(request: Request):
    return templates.TemplateResponse("reset_password.html", {"request": request})

@app.post("/admin/reset_password")
def reset_password_action(
    store_name: str = Form(...),
    username: str = Form(...),
    new_password: str = Form(...),
    db: Session = Depends(get_db),
):
    store_name = store_name.strip()
    username = username.strip()

    store = db.query(Store).filter(func.lower(Store.name) == store_name.lower()).first()
    if not store:
        raise HTTPException(400, "Loja não encontrada. Confira o nome.")

    user = db.query(User).filter(
        User.store_id == store.id,
        func.lower(User.username) == username.lower()
    ).first()
    if not user:
        raise HTTPException(400, "Usuário não encontrado nessa loja.")

    user.password_hash = pwd_context.hash(new_password.strip())
    db.commit()

    return RedirectResponse("/login", status_code=302)

@app.get("/admin/list_users", response_class=PlainTextResponse)
def admin_list_users(db: Session = Depends(get_db)):
    # Mostra lojas e usuários cadastrados (debug rápido)
    stores = db.query(Store).order_by(Store.id.asc()).all()
    lines = []
    for s in stores:
        lines.append(f"STORE {s.id} | {s.name}")
        users = db.query(User).filter(User.store_id == s.id).order_by(User.id.asc()).all()
        for u in users:
            lines.append(f"  USER {u.id} | {u.username} | {u.role}")
    return "\n".join(lines) + "\n"
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    # Se já tiver cookie, manda pro dashboard
    if request.cookies.get("user_id"):
        return RedirectResponse("/dashboard", status_code=302)
    return RedirectResponse("/login", status_code=302)

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
def do_login(
    request: Request,
    store_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db)
):
    store = db.query(Store).filter(Store.name == store_name).first()
    if not store:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Loja não encontrada."})

    user = db.query(User).filter(User.store_id == store.id, User.username == username).first()
    if not user or not pwd_context.verify(password, user.password_hash):
        return templates.TemplateResponse("login.html", {"request": request, "error": "Usuário ou senha inválidos."})

    resp = RedirectResponse("/dashboard", status_code=302)
    resp.set_cookie("user_id", str(user.id), httponly=True, samesite="lax")
    resp.set_cookie("store_id", str(store.id), httponly=True, samesite="lax")
    return resp

@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("user_id")
    resp.delete_cookie("store_id")
    return resp

# ======================
# DASHBOARD
# ======================
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db), user=Depends(require_auth)):
    stats = build_stats(db, user.store_id)

    last_sales = db.query(Sale).filter(Sale.store_id == user.store_id).order_by(Sale.id.desc()).limit(8).all()

    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "user": user, "stats": stats, "last_sales": last_sales}
    )

from datetime import date, datetime
from sqlalchemy import func

def build_stats(db: Session, store_id: int):
    today = date.today()
    month_start = today.replace(day=1)

    # Ajuste os modelos/colunas para os seus nomes reais
    sales_today_value = db.query(func.coalesce(func.sum(Sale.total), 0)).filter(
        Sale.store_id == store_id,
        func.date(Sale.created_at) == today
    ).scalar() or 0

    sales_today_count = db.query(func.count(Sale.id)).filter(
        Sale.store_id == store_id,
        func.date(Sale.created_at) == today
    ).scalar() or 0

    sales_month_value = db.query(func.coalesce(func.sum(Sale.total), 0)).filter(
        Sale.store_id == store_id,
        func.date(Sale.created_at) >= month_start
    ).scalar() or 0

    pending_orders = db.query(func.count(Order.id)).filter(
        Order.store_id == store_id,
        Order.status.in_(["novo", "separando", "saiu"])
    ).scalar() or 0

    low_stock = db.query(func.count(Product.id)).filter(
        Product.store_id == store_id,
        Product.stock <= 3
    ).scalar() or 0

    return {
        "sales_today_value": float(sales_today_value),
        "sales_today_count": int(sales_today_count),
        "sales_month_value": float(sales_month_value),
        "pending_orders": int(pending_orders),
        "low_stock": int(low_stock),
    }
# ======================
# PRODUCTS
# ======================
@app.get("/products", response_class=HTMLResponse)
def products_page(request: Request, user: User = Depends(require_auth), db: Session = Depends(get_db)):
    products = db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.id.desc()).all()
    return templates.TemplateResponse("products.html", {"request": request, "user": user, "products": products})

@app.post("/products/add")
def add_product(
    name: str = Form(...),
    sku: str = Form(""),
    price: float = Form(0.0),
    stock: int = Form(0),
    user: User = Depends(require_auth),
    db: Session = Depends(get_db)
):
    p = Product(store_id=user.store_id, name=name, sku=sku, price=price, stock=stock)
    db.add(p)
    db.commit()
    return RedirectResponse("/products", status_code=302)

@app.post("/products/{product_id}/update")
def update_product(
    product_id: int,
    name: str = Form(...),
    sku: str = Form(""),
    price: float = Form(0.0),
    stock: int = Form(0),
    user: User = Depends(require_auth),
    db: Session = Depends(get_db)
):
    p = db.query(Product).filter(Product.id == product_id, Product.store_id == user.store_id).first()
    if not p:
        raise HTTPException(404, "Produto não encontrado")
    p.name = name
    p.sku = sku
    p.price = price
    p.stock = stock
    db.commit()
    return RedirectResponse("/products", status_code=302)

@app.post("/products/{product_id}/delete")
def delete_product(product_id: int, user: User = Depends(require_auth), db: Session = Depends(get_db)):
    p = db.query(Product).filter(Product.id == product_id, Product.store_id == user.store_id).first()
    if not p:
        raise HTTPException(404, "Produto não encontrado")
    db.delete(p)
    db.commit()
    return RedirectResponse("/products", status_code=302)

# ======================
# CUSTOMERS
# ======================
@app.get("/customers", response_class=HTMLResponse)
def customers_page(request: Request, user: User = Depends(require_auth), db: Session = Depends(get_db)):
    customers = db.query(Customer).filter(Customer.store_id == user.store_id).order_by(Customer.id.desc()).all()
    return templates.TemplateResponse("customers.html", {"request": request, "user": user, "customers": customers})

@app.post("/customers/add")
def add_customer(
    name: str = Form(...),
    phone: str = Form(""),
    user: User = Depends(require_auth),
    db: Session = Depends(get_db)
):
    c = Customer(store_id=user.store_id, name=name, phone=phone)
    db.add(c)
    db.commit()
    return RedirectResponse("/customers", status_code=302)

@app.post("/customers/{customer_id}/delete")
def delete_customer(customer_id: int, user: User = Depends(require_auth), db: Session = Depends(get_db)):
    c = db.query(Customer).filter(Customer.id == customer_id, Customer.store_id == user.store_id).first()
    if not c:
        raise HTTPException(404, "Cliente não encontrado")
    db.delete(c)
    db.commit()
    return RedirectResponse("/customers", status_code=302)

# ======================
# SALES
# ======================
@app.get("/sales", response_class=HTMLResponse)
def sales_page(request: Request, user: User = Depends(require_auth), db: Session = Depends(get_db)):
    sales = db.query(Sale).filter(Sale.store_id == user.store_id).order_by(Sale.id.desc()).limit(200).all()
    return templates.TemplateResponse("sales.html", {"request": request, "user": user, "sales": sales})

@app.get("/sales/new", response_class=HTMLResponse)
def new_sale_page(request: Request, user: User = Depends(require_auth), db: Session = Depends(get_db)):
    products = db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.name.asc()).all()
    return templates.TemplateResponse("sale_new.html", {"request": request, "user": user, "products": products})

@app.post("/sales/create")
def create_sale(
    customer_name: str = Form("Cliente avulso"),
    payment_method: str = Form("dinheiro"),
    # campos repetidos no form: product_id e qty
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
    request: Request = None
):
    form = request._form if hasattr(request, "_form") else None

    # FastAPI não expõe fácil listas sem Request.form(); vamos pegar direto:
    # workaround: usar request.form() sincrono (ok aqui)
    # (FastAPI resolve isso; deixo simples)
    raise HTTPException(400, "Abra o /sales/create via POST usando o formulário (já configurado no HTML).")

@app.post("/sales/create_form")
async def create_sale_form(
    request: Request,
    customer_name: str = Form("Cliente avulso"),
    payment_method: str = Form("dinheiro"),
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
):
    form = await request.form()
    product_ids = form.getlist("product_id")
    qtys = form.getlist("qty")

    items = []
    total = 0.0

    for pid_str, qty_str in zip(product_ids, qtys):
        if not pid_str:
            continue
        pid = int(pid_str)
        qty = int(qty_str) if qty_str else 0
        if qty <= 0:
            continue

        p = db.query(Product).filter(Product.id == pid, Product.store_id == user.store_id).first()
        if not p:
            continue
        if p.stock < qty:
            # estoque insuficiente
            return templates.TemplateResponse(
                "sale_new.html",
                {
                    "request": request,
                    "user": user,
                    "products": db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.name.asc()).all(),
                    "error": f"Estoque insuficiente para {p.name}. Estoque atual: {p.stock}"
                }
            )

        line_total = float(p.price) * qty
        total += line_total
        items.append((p, qty, line_total))

    if not items:
        return templates.TemplateResponse(
            "sale_new.html",
            {
                "request": request,
                "user": user,
                "products": db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.name.asc()).all(),
                "error": "Adicione pelo menos 1 item na venda."
            }
        )

    sale = Sale(
        store_id=user.store_id,
        customer_name=customer_name.strip() if customer_name else "Cliente avulso",
        payment_method=payment_method.strip() if payment_method else "dinheiro",
        total=round(total, 2)
    )
    db.add(sale)
    db.commit()
    db.refresh(sale)

    # grava itens + baixa estoque
    for p, qty, line_total in items:
        p.stock -= qty
        si = SaleItem(
            sale_id=sale.id,
            product_id=p.id,
            product_name=p.name,
            qty=qty,
            unit_price=float(p.price),
            line_total=round(line_total, 2)
        )
        db.add(si)

    db.commit()
    return RedirectResponse("/sales", status_code=302)

@app.get("/sales/{sale_id}", response_class=HTMLResponse)
def sale_detail(request: Request, sale_id: int, user: User = Depends(require_auth), db: Session = Depends(get_db)):
    sale = db.query(Sale).filter(Sale.id == sale_id, Sale.store_id == user.store_id).first()
    if not sale:
        raise HTTPException(404, "Venda não encontrada")
    return templates.TemplateResponse("sale_detail.html", {"request": request, "user": user, "sale": sale})

# ======================
# ADMIN - CREATE NEW STORE + ADMIN USER
# ======================
@app.get("/admin/setup", response_class=HTMLResponse)
def setup_page(request: Request):
    return templates.TemplateResponse("setup.html", {"request": request})

@app.post("/admin/setup")
def setup_store(
    request: Request,
    store_name: str = Form(...),
    admin_user: str = Form(...),
    admin_pass: str = Form(...),
    db: Session = Depends(get_db)
):
    # cria loja e admin
    if db.query(Store).filter(Store.name == store_name).first():
        return templates.TemplateResponse("setup.html", {"request": request, "error": "Já existe uma loja com esse nome."})

    store = Store(name=store_name)
    db.add(store)
    db.commit()
    db.refresh(store)

    user = User(
        store_id=store.id,
        username=admin_user,
        password_hash=pwd_context.hash("admin123"),
        role="admin"
    )
    db.add(user)
    db.commit()

    return templates.TemplateResponse(
        "setup.html",
        {"request": request, "success": f"Loja criada! Agora faça login com Loja='{store_name}', Usuário='{admin_user}'"}
    )

# ======================
# ORDERS (DELIVERY)
# ======================
@app.get("/orders", response_class=HTMLResponse)
def orders_page(
    request: Request,
    status: str | None = None,
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
):
    q = db.query(Order).filter(Order.store_id == user.store_id)

    if status:
        status = status.strip().lower()
        q = q.filter(Order.status == status)

    orders = q.order_by(Order.id.desc()).limit(300).all()

    return templates.TemplateResponse(
        "orders.html",
        {"request": request, "user": user, "orders": orders, "status_filter": status or ""}
    )

@app.get("/orders/new", response_class=HTMLResponse)
def new_order_page(request: Request, user: User = Depends(require_auth), db: Session = Depends(get_db)):
    products = db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.name.asc()).all()
    return templates.TemplateResponse("order_new.html", {"request": request, "user": user, "products": products})


@app.post("/orders/create_form")
async def create_order_form(
    request: Request,
    customer_name: str = Form("Cliente avulso"),
    customer_phone: str = Form(""),
    address: str = Form(""),
    notes: str = Form(""),
    payment_method: str = Form("dinheiro"),
    delivery_fee: float = Form(0.0),
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
):
    form = await request.form()
    product_ids = form.getlist("product_id")
    qtys = form.getlist("qty")

    items = []
    subtotal = 0.0

    for pid_str, qty_str in zip(product_ids, qtys):
        if not pid_str:
            continue
        pid = int(pid_str)
        qty = int(qty_str) if qty_str else 0
        if qty <= 0:
            continue

        p = db.query(Product).filter(Product.id == pid, Product.store_id == user.store_id).first()
        if not p:
            continue

        if p.stock < qty:
            return templates.TemplateResponse(
                "order_new.html",
                {
                    "request": request,
                    "user": user,
                    "products": db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.name.asc()).all(),
                    "error": f"Estoque insuficiente para {p.name}. Estoque atual: {p.stock}"
                }
            )

        line_total = float(p.price) * qty
        subtotal += line_total
        items.append((p, qty, line_total))

    if not items:
        return templates.TemplateResponse(
            "order_new.html",
            {
                "request": request,
                "user": user,
                "products": db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.name.asc()).all(),
                "error": "Adicione pelo menos 1 item no pedido."
            }
        )

    delivery_fee = float(delivery_fee or 0.0)
    total = round(subtotal + delivery_fee, 2)

    order = Order(
        store_id=user.store_id,
        customer_name=(customer_name or "Cliente avulso").strip(),
        customer_phone=(customer_phone or "").strip(),
        address=(address or "").strip(),
        notes=(notes or "").strip(),
        payment_method=(payment_method or "dinheiro").strip(),
        delivery_fee=round(delivery_fee, 2),
        status="novo",
        total=total
    )
    db.add(order)
    db.commit()
    db.refresh(order)

    # grava itens + baixa estoque
    for p, qty, line_total in items:
        p.stock -= qty
        oi = OrderItem(
            order_id=order.id,
            product_id=p.id,
            product_name=p.name,
            qty=qty,
            unit_price=float(p.price),
            line_total=round(line_total, 2)
        )
        db.add(oi)

    db.commit()
    return RedirectResponse("/orders", status_code=302)


@app.get("/orders/{order_id}", response_class=HTMLResponse)
def order_detail(request: Request, order_id: int, user: User = Depends(require_auth), db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.id == order_id, Order.store_id == user.store_id).first()
    if not order:
        raise HTTPException(404, "Pedido não encontrado")
    return templates.TemplateResponse("order_detail.html", {"request": request, "user": user, "order": order})


@app.post("/orders/{order_id}/status")
def update_order_status(
    order_id: int,
    status: str = Form(...),
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
):
    order = db.query(Order).filter(Order.id == order_id, Order.store_id == user.store_id).first()
    if not order:
        raise HTTPException(404, "Pedido não encontrado")

    allowed = {"novo", "separando", "saiu", "entregue", "cancelado"}
    status = (status or "").strip().lower()
    if status not in allowed:
        raise HTTPException(400, "Status inválido")

    order.status = status
    db.commit()

    # Se entregou, cria uma VENDA concluída automaticamente (sem baixar estoque de novo)
    if status == "entregue":
        tag = f"(Pedido #{order.id})"

        existing = (
            db.query(Sale)
            .filter(Sale.store_id == user.store_id, Sale.customer_name.contains(tag))
            .first()
        )

        if not existing:
            sale = Sale(
                store_id=user.store_id,
                customer_name=f"{order.customer_name} {tag}",
                payment_method=order.payment_method,
                total=float(order.total),
            )
            db.add(sale)
            db.commit()
            db.refresh(sale)

            for it in order.items:
                si = SaleItem(
                    sale_id=sale.id,
                    product_id=it.product_id,
                    product_name=it.product_name,
                    qty=it.qty,
                    unit_price=float(it.unit_price),
                    line_total=float(it.line_total),
                )
                db.add(si)

            db.commit()

    return RedirectResponse(f"/orders/{order_id}", status_code=302)