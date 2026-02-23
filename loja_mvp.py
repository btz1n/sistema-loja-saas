import os
import json
import traceback
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text, func
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Session

from passlib.context import CryptContext


# =========================
# App + Paths
# =========================
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI()

# Static (Render precisa existir a pasta)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# =========================
# Template engine (Jinja2)
# =========================
from starlette.templating import Jinja2Templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# =========================
# Password hashing
# =========================
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# =========================
# Database
# =========================
def get_database_url() -> str:
    # Render usa DATABASE_URL. Local pode usar SQLITE_URL.
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        # Render geralmente fornece postgres:// -> SQLAlchemy prefere postgresql+psycopg2://
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
        return db_url

    # fallback local sqlite
    return "sqlite:///./loja.db"


DATABASE_URL = get_database_url()

connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    connect_args = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# =========================
# Models
# =========================
class Store(Base):
    __tablename__ = "stores"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(120), unique=True, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    users = relationship("User", back_populates="store")
    customers = relationship("Customer", back_populates="store")
    products = relationship("Product", back_populates="store")
    orders = relationship("Order", back_populates="store")
    sales = relationship("Sale", back_populates="store")


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    username = Column(String(80), nullable=False)
    role = Column(String(30), default="admin")
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    store = relationship("Store", back_populates="users")


class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    name = Column(String(120), nullable=False)
    phone = Column(String(60), default="")
    address = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    store = relationship("Store", back_populates="customers")


class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    name = Column(String(160), nullable=False)
    price = Column(Float, default=0.0)
    stock = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    store = relationship("Store", back_populates="products")


class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    customer_name = Column(String(160), default="")
    phone = Column(String(60), default="")
    address = Column(Text, default="")
    notes = Column(Text, default="")
    status = Column(String(30), default="novo")  # novo / separando / saiu / entregue
    total = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)

    store = relationship("Store", back_populates="orders")
    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"
    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    product_name = Column(String(160), nullable=False)
    qty = Column(Integer, default=1)
    price = Column(Float, default=0.0)

    order = relationship("Order", back_populates="items")


class Sale(Base):
    __tablename__ = "sales"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    customer_name = Column(String(160), default="")
    status = Column(String(30), default="concluida")
    total = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)

    store = relationship("Store", back_populates="sales")
    items = relationship("SaleItem", back_populates="sale", cascade="all, delete-orphan")


class SaleItem(Base):
    __tablename__ = "sale_items"
    id = Column(Integer, primary_key=True, index=True)
    sale_id = Column(Integer, ForeignKey("sales.id"), nullable=False)
    product_name = Column(String(160), nullable=False)
    qty = Column(Integer, default=1)
    price = Column(Float, default=0.0)

    sale = relationship("Sale", back_populates="items")


# =========================
# Create tables
# =========================
def init_db():
    Base.metadata.create_all(bind=engine)


init_db()


# =========================
# Error capture for debug
# =========================
LAST_ERROR = None

@app.middleware("http")
async def log_exceptions(request: Request, call_next):
    global LAST_ERROR
    try:
        return await call_next(request)
    except Exception:
        LAST_ERROR = {"url": str(request.url), "trace": traceback.format_exc()}
        print("=== EXCEPTION ===")
        print("URL:", request.url)
        print(LAST_ERROR["trace"])
        raise


@app.get("/debug/last_error", response_class=PlainTextResponse)
def debug_last_error():
    global LAST_ERROR
    if not LAST_ERROR:
        return "Sem erro capturado ainda."
    return f"URL: {LAST_ERROR['url']}\n\n{LAST_ERROR['trace']}"


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(func.now())
        return {"ok": True, "db": "ok"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/debug/whoami", response_class=PlainTextResponse)
def whoami(request: Request):
    return f"user_id={request.cookies.get('user_id')} store_id={request.cookies.get('store_id')}"


# =========================
# Auth helpers
# =========================
def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return False


def hash_password(plain_password: str) -> str:
    # bcrypt tem limite prático ~72 bytes. Passlib lida, mas é bom evitar senha gigante.
    if len(plain_password.encode("utf-8")) > 72:
        raise HTTPException(status_code=400, detail="Senha muito grande (limite 72 bytes).")
    return pwd_context.hash(plain_password)


def set_session_cookies(resp, user, request)
    xf_proto = request.headers.get("x-forwarded-proto", "")
    is_https = (request.url.scheme == "https") or (xf_proto == "https")

    resp.set_cookie("user_id", str(user.id), httponly=True, samesite="lax", secure=is_https, max_age=60*60*24*30)
    resp.set_cookie("store_id", str(user.store_id), httponly=True, samesite="lax", secure=is_https, max_age=60*60*24*30)
    return resp


def clear_session_cookies(resp: RedirectResponse):
    resp.delete_cookie("user_id")
    resp.delete_cookie("store_id")
    return resp


def require_auth(request: Request, db: Session = Depends(get_db)) -> User:
    user_id = request.cookies.get("user_id")
    store_id = request.cookies.get("store_id")
    if not user_id or not store_id:
        raise HTTPException(status_code=401, detail="Não autenticado.")

    user = (
        db.query(User)
        .filter(User.id == int(user_id), User.store_id == int(store_id))
        .first()
    )
    if not user:
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    return user


# =========================
# Helpers
# =========================
def get_store_by_name(db: Session, store_name: str) -> Optional[Store]:
    if not store_name:
        return None
    return db.query(Store).filter(func.lower(Store.name) == store_name.strip().lower()).first()


def parse_items_json(items_json: str) -> List[Dict[str, Any]]:
    try:
        data = json.loads(items_json)
        if not isinstance(data, list):
            return []
        cleaned = []
        for it in data:
            pname = str(it.get("product_name", "")).strip()
            qty = int(it.get("qty", 0) or 0)
            price = float(it.get("price", 0) or 0)
            if pname and qty > 0:
                cleaned.append({"product_name": pname, "qty": qty, "price": price})
        return cleaned
    except Exception:
        return []


def calc_total(items: List[Dict[str, Any]]) -> float:
    total = 0.0
    for it in items:
        total += float(it["qty"]) * float(it["price"])
    return round(total, 2)


def make_sale_from_order(db: Session, order: Order) -> Sale:
    sale = Sale(
        store_id=order.store_id,
        customer_name=order.customer_name,
        status="concluida",
        total=order.total,
        created_at=datetime.utcnow(),
    )
    db.add(sale)
    db.flush()  # pega sale.id

    for it in order.items:
        db.add(SaleItem(
            sale_id=sale.id,
            product_name=it.product_name,
            qty=it.qty,
            price=it.price,
        ))

    return sale


# =========================
# Routes (public)
# =========================
@app.get("/", response_class=HTMLResponse)
def root():
    return RedirectResponse(url="/login", status_code=302)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "user": None})


@app.post("/login", response_class=HTMLResponse)
def login_action(
    request: Request,
    db: Session = Depends(get_db),
    store_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
):
    store = get_store_by_name(db, store_name)
    if not store:
        return templates.TemplateResponse("login.html", {"request": request, "user": None, "error": "Loja não encontrada."})

    user = db.query(User).filter(User.store_id == store.id, func.lower(User.username) == username.strip().lower()).first()
    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse("login.html", {"request": request, "user": None, "error": "Usuário ou senha inválidos."})

    resp = RedirectResponse(url="/dashboard", status_code=302)
    set_session_cookies(resp, user)
    return resp


@app.get("/logout")
def logout():
    resp = RedirectResponse(url="/login", status_code=302)
    return clear_session_cookies(resp)


# =========================
# Admin setup routes (public)
# =========================
@app.get("/admin/setup", response_class=HTMLResponse)
def admin_setup_page(request: Request):
    return templates.TemplateResponse("setup.html", {"request": request, "user": None})


@app.post("/admin/setup", response_class=HTMLResponse)
def admin_setup_action(
    request: Request,
    db: Session = Depends(get_db),
    store_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
):
    store_name = store_name.strip()
    username = username.strip()

    existing = get_store_by_name(db, store_name)
    if existing:
        return templates.TemplateResponse("setup.html", {"request": request, "user": None, "error": "Essa loja já existe."})

    store = Store(name=store_name)
    db.add(store)
    db.flush()

    user = User(
        store_id=store.id,
        username=username,
        role="admin",
        password_hash=hash_password(password),
    )
    db.add(user)
    db.commit()

    resp = RedirectResponse(url="/dashboard", status_code=302)
    set_session_cookies(resp, user)
    return resp


@app.get("/admin/create_user", response_class=HTMLResponse)
def admin_create_user_page(request: Request):
    return templates.TemplateResponse("create_user.html", {"request": request, "user": None})


@app.post("/admin/create_user", response_class=HTMLResponse)
def admin_create_user_action(
    request: Request,
    db: Session = Depends(get_db),
    store_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
):
    store = get_store_by_name(db, store_name)
    if not store:
        return templates.TemplateResponse("create_user.html", {"request": request, "user": None, "error": "Loja não encontrada."})

    username_clean = username.strip()
    exists = db.query(User).filter(User.store_id == store.id, func.lower(User.username) == username_clean.lower()).first()
    if exists:
        return templates.TemplateResponse("create_user.html", {"request": request, "user": None, "error": "Usuário já existe nessa loja."})

    user = User(store_id=store.id, username=username_clean, role="admin", password_hash=hash_password(password))
    db.add(user)
    db.commit()

    return RedirectResponse(url="/login", status_code=302)


@app.get("/admin/reset_password", response_class=HTMLResponse)
def admin_reset_password_page(request: Request):
    return templates.TemplateResponse("reset_password.html", {"request": request, "user": None})


@app.post("/admin/reset_password", response_class=HTMLResponse)
def admin_reset_password_action(
    request: Request,
    db: Session = Depends(get_db),
    store_name: str = Form(...),
    username: str = Form(...),
    new_password: str = Form(...),
):
    store = get_store_by_name(db, store_name)
    if not store:
        return templates.TemplateResponse("reset_password.html", {"request": request, "user": None, "error": "Loja não encontrada."})

    user = db.query(User).filter(User.store_id == store.id, func.lower(User.username) == username.strip().lower()).first()
    if not user:
        return templates.TemplateResponse("reset_password.html", {"request": request, "user": None, "error": "Usuário não encontrado."})

    user.password_hash = hash_password(new_password)
    db.commit()
    return RedirectResponse(url="/login", status_code=302)


# =========================
# App routes (authenticated)
# =========================
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db), user: User = Depends(require_auth)):
    # stats simples
    today = date.today()
    start_month = date(today.year, today.month, 1)

    sales_today = db.query(Sale).filter(Sale.store_id == user.store_id, func.date(Sale.created_at) == today).all()
    sales_month = db.query(Sale).filter(Sale.store_id == user.store_id, Sale.created_at >= start_month).all()

    pending_orders = db.query(Order).filter(
        Order.store_id == user.store_id,
        Order.status.in_(["novo", "separando", "saiu"])
    ).count()

    low_stock = db.query(Product).filter(Product.store_id == user.store_id, Product.stock <= 3).count()

    stats = {
        "sales_today_count": len(sales_today),
        "sales_today_value": sum([s.total for s in sales_today]) if sales_today else 0.0,
        "sales_month_value": sum([s.total for s in sales_month]) if sales_month else 0.0,
        "pending_orders": pending_orders,
        "low_stock": low_stock,
    }

    last_sales = db.query(Sale).filter(Sale.store_id == user.store_id).order_by(Sale.id.desc()).limit(10).all()

    # anexar store_name no user para o template (sem mexer no modelo)
    store = db.query(Store).filter(Store.id == user.store_id).first()
    user.store_name = store.name if store else ""

    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "user": user, "stats": stats, "last_sales": last_sales}
    )


# ---- Products ----
@app.get("/products", response_class=HTMLResponse)
def products_page(request: Request, db: Session = Depends(get_db), user: User = Depends(require_auth)):
    products = db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.id.desc()).all()
    store = db.query(Store).filter(Store.id == user.store_id).first()
    user.store_name = store.name if store else ""
    return templates.TemplateResponse("products.html", {"request": request, "user": user, "products": products})


@app.post("/products/create")
def products_create(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_auth),
    name: str = Form(...),
    price: float = Form(0.0),
    stock: int = Form(0),
):
    p = Product(store_id=user.store_id, name=name.strip(), price=float(price), stock=int(stock))
    db.add(p)
    db.commit()
    return RedirectResponse(url="/products", status_code=302)


# ---- Customers ----
@app.get("/customers", response_class=HTMLResponse)
def customers_page(request: Request, db: Session = Depends(get_db), user: User = Depends(require_auth)):
    customers = db.query(Customer).filter(Customer.store_id == user.store_id).order_by(Customer.id.desc()).all()
    store = db.query(Store).filter(Store.id == user.store_id).first()
    user.store_name = store.name if store else ""
    return templates.TemplateResponse("customers.html", {"request": request, "user": user, "customers": customers})


@app.post("/customers/create")
def customers_create(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_auth),
    name: str = Form(...),
    phone: str = Form(""),
    address: str = Form(""),
):
    c = Customer(store_id=user.store_id, name=name.strip(), phone=phone.strip(), address=address.strip())
    db.add(c)
    db.commit()
    return RedirectResponse(url="/customers", status_code=302)


# ---- Orders ----
@app.get("/orders", response_class=HTMLResponse)
def orders_page(request: Request, db: Session = Depends(get_db), user: User = Depends(require_auth)):
    orders = db.query(Order).filter(Order.store_id == user.store_id).order_by(Order.id.desc()).all()
    store = db.query(Store).filter(Store.id == user.store_id).first()
    user.store_name = store.name if store else ""
    return templates.TemplateResponse("orders.html", {"request": request, "user": user, "orders": orders})


@app.get("/orders/new", response_class=HTMLResponse)
def orders_new_page(request: Request, db: Session = Depends(get_db), user: User = Depends(require_auth)):
    store = db.query(Store).filter(Store.id == user.store_id).first()
    user.store_name = store.name if store else ""
    return templates.TemplateResponse("order_new.html", {"request": request, "user": user})


@app.post("/orders/create")
def orders_create(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_auth),
    customer_name: str = Form(""),
    phone: str = Form(""),
    address: str = Form(""),
    notes: str = Form(""),
    items_json: str = Form(...),
):
    items = parse_items_json(items_json)
    if not items:
        raise HTTPException(status_code=400, detail="Itens inválidos.")

    total = calc_total(items)

    order = Order(
        store_id=user.store_id,
        customer_name=customer_name.strip(),
        phone=phone.strip(),
        address=address.strip(),
        notes=notes.strip(),
        status="novo",
        total=total,
    )
    db.add(order)
    db.flush()

    for it in items:
        db.add(OrderItem(
            order_id=order.id,
            product_name=it["product_name"],
            qty=int(it["qty"]),
            price=float(it["price"]),
        ))

    db.commit()
    return RedirectResponse(url="/orders", status_code=302)


@app.get("/orders/{order_id}", response_class=HTMLResponse)
def order_detail(request: Request, order_id: int, db: Session = Depends(get_db), user: User = Depends(require_auth)):
    order = db.query(Order).filter(Order.store_id == user.store_id, Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Pedido não encontrado.")

    store = db.query(Store).filter(Store.id == user.store_id).first()
    user.store_name = store.name if store else ""

    return templates.TemplateResponse("order_detail.html", {"request": request, "user": user, "order": order})


@app.post("/orders/{order_id}/status")
def order_set_status(
    request: Request,
    order_id: int,
    status: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_auth),
):
    allowed = {"novo", "separando", "saiu", "entregue"}
    if status not in allowed:
        raise HTTPException(status_code=400, detail="Status inválido.")

    order = db.query(Order).filter(Order.store_id == user.store_id, Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Pedido não encontrado.")

    prev = order.status
    order.status = status

    # Se virou entregue, cria venda automaticamente (apenas 1 vez)
    if prev != "entregue" and status == "entregue":
        make_sale_from_order(db, order)

    db.commit()
    return RedirectResponse(url=f"/orders/{order_id}", status_code=302)


# ---- Sales ----
@app.get("/sales", response_class=HTMLResponse)
def sales_page(request: Request, db: Session = Depends(get_db), user: User = Depends(require_auth)):
    sales = db.query(Sale).filter(Sale.store_id == user.store_id).order_by(Sale.id.desc()).all()
    store = db.query(Store).filter(Store.id == user.store_id).first()
    user.store_name = store.name if store else ""
    return templates.TemplateResponse("sales.html", {"request": request, "user": user, "sales": sales})


@app.get("/sales/new", response_class=HTMLResponse)
def sales_new_page(request: Request, db: Session = Depends(get_db), user: User = Depends(require_auth)):
    store = db.query(Store).filter(Store.id == user.store_id).first()
    user.store_name = store.name if store else ""
    return templates.TemplateResponse("sale_new.html", {"request": request, "user": user})


@app.post("/sales/create")
def sales_create(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_auth),
    customer_name: str = Form(""),
    items_json: str = Form(...),
):
    items = parse_items_json(items_json)
    if not items:
        raise HTTPException(status_code=400, detail="Itens inválidos.")
    total = calc_total(items)

    sale = Sale(
        store_id=user.store_id,
        customer_name=customer_name.strip(),
        status="concluida",
        total=total,
        created_at=datetime.utcnow(),
    )
    db.add(sale)
    db.flush()

    for it in items:
        db.add(SaleItem(
            sale_id=sale.id,
            product_name=it["product_name"],
            qty=int(it["qty"]),
            price=float(it["price"]),
        ))

    db.commit()
    return RedirectResponse(url="/sales", status_code=302)


@app.get("/sales/{sale_id}", response_class=HTMLResponse)
def sale_detail(request: Request, sale_id: int, db: Session = Depends(get_db), user: User = Depends(require_auth)):
    sale = db.query(Sale).filter(Sale.store_id == user.store_id, Sale.id == sale_id).first()
    if not sale:
        raise HTTPException(status_code=404, detail="Venda não encontrada.")

    store = db.query(Store).filter(Store.id == user.store_id).first()
    user.store_name = store.name if store else ""

    return templates.TemplateResponse("sales_detail.html", {"request": request, "user": user, "sale": sale})