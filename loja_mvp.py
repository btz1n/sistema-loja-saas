import os
import json
import traceback
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from passlib.context import CryptContext

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, ForeignKey, Text, func
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, Session

LAST_ERROR = None
# ----------------------------
# Paths (absolute, Render-safe)
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------
# App
# ----------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ----------------------------
# DB
# ----------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./loja.db"
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )
else:
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/health")
def health(db: Session = Depends(get_db)):
    # testa conexão DB e se tabelas existem
    try:
        db.execute(func.now())  # só pra bater no banco
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/debug/last_error", response_class=PlainTextResponse)
def debug_last_error():
    global LAST_ERROR
    if not LAST_ERROR:
        return "Sem erro capturado ainda. Acesse /login e depois volte aqui."
    return f"URL: {LAST_ERROR['url']}\n\n{LAST_ERROR['trace']}"
# ----------------------------
# Models
# ----------------------------
class Store(Base):
    __tablename__ = "stores"
    id = Column(Integer, primary_key=True)
    name = Column(String(200), unique=True, nullable=False)

    users = relationship("User", back_populates="store")
    customers = relationship("Customer", back_populates="store")
    products = relationship("Product", back_populates="store")
    sales = relationship("Sale", back_populates="store")
    orders = relationship("Order", back_populates="store")


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    username = Column(String(120), nullable=False)
    role = Column(String(30), default="admin")
    password_hash = Column(String(300), nullable=False)

    store = relationship("Store", back_populates="users")


class Customer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    name = Column(String(200), nullable=False)
    phone = Column(String(80), default="")
    address = Column(Text, default="")

    store = relationship("Store", back_populates="customers")


class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    name = Column(String(200), nullable=False)
    price = Column(Float, default=0.0)
    stock = Column(Integer, default=0)

    store = relationship("Store", back_populates="products")


class Sale(Base):
    __tablename__ = "sales"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    created_at = Column(Date, default=date.today)
    customer_name = Column(String(200), default="")
    total = Column(Float, default=0.0)
    status = Column(String(40), default="concluida")

    store = relationship("Store", back_populates="sales")
    items = relationship("SaleItem", back_populates="sale", cascade="all, delete-orphan")


class SaleItem(Base):
    __tablename__ = "sale_items"
    id = Column(Integer, primary_key=True)
    sale_id = Column(Integer, ForeignKey("sales.id"), nullable=False)
    product_name = Column(String(200), nullable=False)
    qty = Column(Integer, default=1)
    price = Column(Float, default=0.0)

    sale = relationship("Sale", back_populates="items")


class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    created_at = Column(Date, default=date.today)

    customer_name = Column(String(200), default="")
    phone = Column(String(80), default="")
    address = Column(Text, default="")

    status = Column(String(40), default="novo")  # novo, separando, saiu, entregue
    total = Column(Float, default=0.0)
    notes = Column(Text, default="")

    store = relationship("Store", back_populates="orders")
    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    __tablename__ = "order_items"
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    product_name = Column(String(200), nullable=False)
    qty = Column(Integer, default=1)
    price = Column(Float, default=0.0)

    order = relationship("Order", back_populates="items")


Base.metadata.create_all(bind=engine)


# ----------------------------
# Middleware: always log exceptions (Render)
# ----------------------------
@app.middleware("http")
async def log_exceptions(request: Request, call_next):
    global LAST_ERROR
    try:
        return await call_next(request)
    except Exception:
        LAST_ERROR = {
            "url": str(request.url),
            "trace": traceback.format_exc()
        }
        print("=== EXCEPTION ===")
        print("URL:", request.url)
        print(LAST_ERROR["trace"])
        raise

# ----------------------------
# Auth (DO NOT redirect inside dependency)
# ----------------------------
def require_auth(request: Request, db: Session):
    user_id = request.cookies.get("user_id")
    store_id = request.cookies.get("store_id")
    if not user_id or not store_id:
        raise HTTPException(status_code=401, detail="Não autenticado.")

    try:
        uid = int(user_id)
        sid = int(store_id)
    except ValueError:
        raise HTTPException(status_code=401, detail="Sessão inválida.")

    user = db.query(User).filter(User.id == uid, User.store_id == sid).first()
    if not user:
        raise HTTPException(status_code=401, detail="Sessão inválida.")
    return user


def get_current_user_optional(request: Request, db: Session) -> Optional[User]:
    try:
        return require_auth(request, db)
    except HTTPException:
        return None


def redirect_login():
    return RedirectResponse("/login", status_code=302)


def set_session_cookies(resp: RedirectResponse, user: User):
    # In Render HTTPS, secure=True helps. Locally may block cookies if not https.
    secure = bool(os.environ.get("RENDER")) or bool(os.environ.get("ON_RENDER"))
    resp.set_cookie("user_id", str(user.id), httponly=True, samesite="lax", secure=secure)
    resp.set_cookie("store_id", str(user.store_id), httponly=True, samesite="lax", secure=secure)
    return resp


# ----------------------------
# Public routes
# ----------------------------
@app.get("/", include_in_schema=False)
def home():
    return RedirectResponse("/login", status_code=302)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "user": None})


@app.post("/login", response_class=HTMLResponse)
def do_login(
    request: Request,
    store_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    store_name = store_name.strip()
    username = username.strip()
    password = password.strip()

    store = db.query(Store).filter(func.lower(Store.name) == store_name.lower()).first()
    if not store:
        return templates.TemplateResponse("login.html", {"request": request, "user": None, "error": "Loja não encontrada."})

    user = db.query(User).filter(
        User.store_id == store.id,
        func.lower(User.username) == username.lower()
    ).first()

    if not user or not pwd_context.verify(password, user.password_hash):
        return templates.TemplateResponse("login.html", {"request": request, "user": None, "error": "Usuário ou senha inválidos."})

    resp = RedirectResponse("/dashboard", status_code=302)
    return set_session_cookies(resp, user)


@app.get("/logout")
def logout():
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie("user_id")
    resp.delete_cookie("store_id")
    return resp


# ----------------------------
# Admin utilities
# ----------------------------
@app.get("/admin/list_users", response_class=PlainTextResponse)
def admin_list_users(db: Session = Depends(get_db)):
    stores = db.query(Store).order_by(Store.id.asc()).all()
    lines = []
    for s in stores:
        lines.append(f"STORE {s.id} | {s.name}")
        users = db.query(User).filter(User.store_id == s.id).order_by(User.id.asc()).all()
        for u in users:
            lines.append(f"  USER {u.id} | {u.username} | {u.role}")
    return "\n".join(lines) + "\n"


@app.get("/admin/setup", response_class=HTMLResponse)
def admin_setup_page(request: Request):
    return templates.TemplateResponse("setup.html", {"request": request, "user": None})


@app.post("/admin/setup")
def admin_setup_create(
    request: Request,
    store_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    store_name = store_name.strip()
    username = username.strip()
    password = password.strip()

    store = db.query(Store).filter(func.lower(Store.name) == store_name.lower()).first()
    if not store:
        store = Store(name=store_name)
        db.add(store)
        db.commit()
        db.refresh(store)

    existing = db.query(User).filter(
        User.store_id == store.id,
        func.lower(User.username) == username.lower()
    ).first()
    if existing:
        return RedirectResponse("/login", status_code=302)

    u = User(
        store_id=store.id,
        username=username,
        role="admin",
        password_hash=pwd_context.hash(password),
    )
    db.add(u)
    db.commit()
    return RedirectResponse("/login", status_code=302)


@app.get("/admin/create_user", response_class=HTMLResponse)
def admin_create_user_page(request: Request):
    return templates.TemplateResponse("create_user.html", {"request": request, "user": None})


@app.post("/admin/create_user")
def admin_create_user(
    request: Request,
    store_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    store_name = store_name.strip()
    username = username.strip()
    password = password.strip()

    store = db.query(Store).filter(func.lower(Store.name) == store_name.lower()).first()
    if not store:
        raise HTTPException(400, "Loja não encontrada. Confira o nome.")

    existing = db.query(User).filter(
        User.store_id == store.id,
        func.lower(User.username) == username.lower()
    ).first()
    if existing:
        raise HTTPException(400, "Usuário já existe nessa loja.")

    u = User(store_id=store.id, username=username, role="admin", password_hash=pwd_context.hash(password))
    db.add(u)
    db.commit()
    return RedirectResponse("/login", status_code=302)


@app.get("/admin/reset_password", response_class=HTMLResponse)
def reset_password_page(request: Request):
    return templates.TemplateResponse("reset_password.html", {"request": request, "user": None})


@app.post("/admin/reset_password")
def reset_password_action(
    request: Request,
    store_name: str = Form(...),
    username: str = Form(...),
    new_password: str = Form(...),
    db: Session = Depends(get_db),
):
    store_name = store_name.strip()
    username = username.strip()
    new_password = new_password.strip()

    store = db.query(Store).filter(func.lower(Store.name) == store_name.lower()).first()
    if not store:
        raise HTTPException(400, "Loja não encontrada. Confira o nome.")

    user = db.query(User).filter(
        User.store_id == store.id,
        func.lower(User.username) == username.lower()
    ).first()
    if not user:
        raise HTTPException(400, "Usuário não encontrado nessa loja.")

    user.password_hash = pwd_context.hash(new_password)
    db.commit()
    return RedirectResponse("/login", status_code=302)


# ----------------------------
# Dashboard stats
# ----------------------------
def build_stats(db: Session, store_id: int) -> Dict[str, Any]:
    today = date.today()
    month_start = today.replace(day=1)

    sales_today_value = db.query(func.coalesce(func.sum(Sale.total), 0.0)).filter(
        Sale.store_id == store_id,
        Sale.created_at == today
    ).scalar() or 0.0

    sales_today_count = db.query(func.count(Sale.id)).filter(
        Sale.store_id == store_id,
        Sale.created_at == today
    ).scalar() or 0

    sales_month_value = db.query(func.coalesce(func.sum(Sale.total), 0.0)).filter(
        Sale.store_id == store_id,
        Sale.created_at >= month_start
    ).scalar() or 0.0

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


# ----------------------------
# Private pages (match your templates)
# ----------------------------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    stats = build_stats(db, user.store_id)

    # last sales for dashboard
    last_sales = db.query(Sale).filter(Sale.store_id == user.store_id).order_by(Sale.id.desc()).limit(8).all()

    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "user": user, "stats": stats, "last_sales": last_sales},
    )


@app.get("/products", response_class=HTMLResponse)
def products_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    products = db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.id.desc()).all()
    return templates.TemplateResponse("products.html", {"request": request, "user": user, "products": products})


@app.post("/products/create")
def products_create(
    request: Request,
    name: str = Form(...),
    price: float = Form(0.0),
    stock: int = Form(0),
    db: Session = Depends(get_db),
):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    p = Product(store_id=user.store_id, name=name.strip(), price=float(price), stock=int(stock))
    db.add(p)
    db.commit()
    return RedirectResponse("/products", status_code=302)


@app.get("/customers", response_class=HTMLResponse)
def customers_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    customers = db.query(Customer).filter(Customer.store_id == user.store_id).order_by(Customer.id.desc()).all()
    return templates.TemplateResponse("customers.html", {"request": request, "user": user, "customers": customers})


@app.post("/customers/create")
def customers_create(
    request: Request,
    name: str = Form(...),
    phone: str = Form(""),
    address: str = Form(""),
    db: Session = Depends(get_db),
):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    c = Customer(store_id=user.store_id, name=name.strip(), phone=phone.strip(), address=address.strip())
    db.add(c)
    db.commit()
    return RedirectResponse("/customers", status_code=302)


@app.get("/sales", response_class=HTMLResponse)
def sales_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    sales = db.query(Sale).filter(Sale.store_id == user.store_id).order_by(Sale.id.desc()).all()
    return templates.TemplateResponse("sales.html", {"request": request, "user": user, "sales": sales})


@app.get("/sales/{sale_id}", response_class=HTMLResponse)
def sale_detail_page(sale_id: int, request: Request, db: Session = Depends(get_db)):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    sale = db.query(Sale).filter(Sale.id == sale_id, Sale.store_id == user.store_id).first()
    if not sale:
        raise HTTPException(404, "Venda não encontrada.")
    return templates.TemplateResponse("sales_detail.html", {"request": request, "user": user, "sale": sale})


@app.get("/sales/new", response_class=HTMLResponse)
def new_sale_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    products = db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.name.asc()).all()
    customers = db.query(Customer).filter(Customer.store_id == user.store_id).order_by(Customer.name.asc()).all()

    return templates.TemplateResponse("sale_new.html", {"request": request, "user": user, "products": products, "customers": customers})


@app.post("/sales/create")
def sales_create(
    request: Request,
    customer_name: str = Form(""),
    items_json: str = Form(...),
    db: Session = Depends(get_db),
):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    try:
        items = json.loads(items_json)
        if not isinstance(items, list):
            raise ValueError
    except Exception:
        raise HTTPException(400, "Itens inválidos.")

    sale = Sale(store_id=user.store_id, customer_name=customer_name.strip(), created_at=date.today(), status="concluida")
    db.add(sale)
    db.commit()
    db.refresh(sale)

    total = 0.0
    for it in items:
        pname = str(it.get("product_name", "")).strip()
        qty = int(it.get("qty", 0) or 0)
        price = float(it.get("price", 0.0) or 0.0)
        if not pname or qty <= 0:
            continue

        total += qty * price
        db.add(SaleItem(sale_id=sale.id, product_name=pname, qty=qty, price=price))

        prod = db.query(Product).filter(Product.store_id == user.store_id, func.lower(Product.name) == pname.lower()).first()
        if prod:
            prod.stock = max(0, int(prod.stock) - qty)

    sale.total = float(total)
    db.commit()

    return RedirectResponse("/sales", status_code=302)


@app.get("/orders", response_class=HTMLResponse)
def orders_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    orders = db.query(Order).filter(Order.store_id == user.store_id).order_by(Order.id.desc()).all()
    return templates.TemplateResponse("orders.html", {"request": request, "user": user, "orders": orders})


@app.get("/orders/new", response_class=HTMLResponse)
def new_order_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    customers = db.query(Customer).filter(Customer.store_id == user.store_id).order_by(Customer.name.asc()).all()
    products = db.query(Product).filter(Product.store_id == user.store_id).order_by(Product.name.asc()).all()

    return templates.TemplateResponse("order_new.html", {"request": request, "user": user, "customers": customers, "products": products})


@app.get("/orders/{order_id}", response_class=HTMLResponse)
def order_detail_page(order_id: int, request: Request, db: Session = Depends(get_db)):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    order = db.query(Order).filter(Order.id == order_id, Order.store_id == user.store_id).first()
    if not order:
        raise HTTPException(404, "Pedido não encontrado.")
    return templates.TemplateResponse("order_detail.html", {"request": request, "user": user, "order": order})


@app.post("/orders/create")
def orders_create(
    request: Request,
    customer_name: str = Form(""),
    phone: str = Form(""),
    address: str = Form(""),
    notes: str = Form(""),
    items_json: str = Form(...),
    db: Session = Depends(get_db),
):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    try:
        items = json.loads(items_json)
        if not isinstance(items, list):
            raise ValueError
    except Exception:
        raise HTTPException(400, "Itens inválidos.")

    order = Order(
        store_id=user.store_id,
        created_at=date.today(),
        customer_name=customer_name.strip(),
        phone=phone.strip(),
        address=address.strip(),
        notes=notes.strip(),
        status="novo",
    )
    db.add(order)
    db.commit()
    db.refresh(order)

    total = 0.0
    for it in items:
        pname = str(it.get("product_name", "")).strip()
        qty = int(it.get("qty", 0) or 0)
        price = float(it.get("price", 0.0) or 0.0)
        if not pname or qty <= 0:
            continue

        total += qty * price
        db.add(OrderItem(order_id=order.id, product_name=pname, qty=qty, price=price))

        prod = db.query(Product).filter(Product.store_id == user.store_id, func.lower(Product.name) == pname.lower()).first()
        if prod:
            prod.stock = max(0, int(prod.stock) - qty)

    order.total = float(total)
    db.commit()

    return RedirectResponse("/orders", status_code=302)


@app.post("/orders/{order_id}/status")
def order_set_status(
    order_id: int,
    request: Request,
    status: str = Form(...),
    db: Session = Depends(get_db),
):
    user = get_current_user_optional(request, db)
    if not user:
        return redirect_login()

    order = db.query(Order).filter(Order.id == order_id, Order.store_id == user.store_id).first()
    if not order:
        raise HTTPException(404, "Pedido não encontrado.")

    status = status.strip().lower()
    if status not in ["novo", "separando", "saiu", "entregue"]:
        raise HTTPException(400, "Status inválido.")

    order.status = status
    db.commit()

    # Auto-create sale when delivered
    if status == "entregue":
        sale = Sale(
            store_id=user.store_id,
            created_at=date.today(),
            customer_name=order.customer_name,
            total=order.total,
            status="concluida",
        )
        db.add(sale)
        db.commit()
        db.refresh(sale)

        for it in order.items:
            db.add(SaleItem(sale_id=sale.id, product_name=it.product_name, qty=it.qty, price=it.price))
        db.commit()

    return RedirectResponse(f"/orders/{order_id}", status_code=302)