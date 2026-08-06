import asyncio
import base64
import hashlib
import hmac
import json
import math
import os
import secrets
import string
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, DateTime, ForeignKey, String, create_engine, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./carkie.db")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Convoy(Base):
    __tablename__ = "convoys"

    code: Mapped[str] = mapped_column(String(6), primary_key=True)
    creator_id: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False, default="Untitled Trip")
    destination: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    started: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    members: Mapped[list["ConvoyMember"]] = relationship(cascade="all, delete-orphan", order_by="ConvoyMember.joined_at")


class ConvoyMember(Base):
    __tablename__ = "convoy_members"

    convoy_code: Mapped[str] = mapped_column(ForeignKey("convoys.code", ondelete="CASCADE"), primary_key=True)
    member_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    car_name: Mapped[str] = mapped_column(String(120), nullable=False)
    car_number: Mapped[str] = mapped_column(String(40), nullable=False)
    joined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    phone_number: Mapped[str] = mapped_column(String(40), nullable=False, default="")
    car_name: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    car_number: Mapped[str] = mapped_column(String(40), nullable=False, default="")


class RegisterInput(BaseModel):
    email: str = Field(min_length=3, max_length=255)
    password: str = Field(min_length=8, max_length=128)
    name: str = Field(min_length=1, max_length=120)
    phone_number: str = Field(min_length=1, max_length=40)
    car_name: str = Field(min_length=1, max_length=120)
    car_number: str = Field(min_length=1, max_length=40)


class LoginInput(BaseModel):
    email: str
    password: str


class ProfileUpdateInput(BaseModel):
    email: str = Field(min_length=3, max_length=255)
    name: str = Field(min_length=1, max_length=120)
    phone_number: str = Field(default="", max_length=40)
    car_name: str = Field(min_length=1, max_length=120)
    car_number: str = Field(min_length=1, max_length=40)


class MemberInput(BaseModel):
    member_id: str = Field(min_length=1, max_length=255)
    name: str = Field(min_length=1, max_length=120)
    car_name: str = Field(min_length=1, max_length=120)
    car_number: str = Field(min_length=1, max_length=40)


class CreateConvoyInput(MemberInput):
    pass


class StartInput(BaseModel):
    member_id: str


class MemberActionInput(BaseModel):
    member_id: str


class UpdateConvoyInput(BaseModel):
    member_id: str
    name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    destination: Optional[str] = Field(default=None, max_length=255)


class ConnectionManager:
    def __init__(self):
        self.rooms: dict[str, dict[str, WebSocket]] = {}
        self.locations: dict[str, dict[str, dict]] = {}

    async def connect(self, code: str, member_id: str, websocket: WebSocket):
        await websocket.accept()
        self.rooms.setdefault(code, {})[member_id] = websocket
        await websocket.send_text(json.dumps({"type": "location_snapshot", "locations": self.locations.get(code, {})}))
        await self.broadcast_presence(code)

    def disconnect(self, code: str, member_id: str):
        room = self.rooms.get(code)
        if room:
            room.pop(member_id, None)
            if not room:
                self.rooms.pop(code, None)

    def online_members(self, code: str) -> list[str]:
        return list(self.rooms.get(code, {}).keys())

    async def broadcast_presence(self, code: str):
        await self.broadcast(code, {"type": "presence", "online": self.online_members(code)})

    async def broadcast(self, code: str, message: dict):
        room = self.rooms.get(code, {})
        payload = json.dumps(message)
        dead: list[str] = []
        for member_id, ws in room.items():
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(member_id)
        for member_id in dead:
            room.pop(member_id, None)

    async def broadcast_room_updated(self, code: str, room_data: dict):
        await self.broadcast(code, {"type": "room_updated", "room": room_data})

    async def broadcast_member_kicked(self, code: str, member_id: str):
        await self.broadcast(code, {"type": "member_kicked", "member_id": member_id})

    async def broadcast_member_left(self, code: str, member_id: str):
        self.locations.get(code, {}).pop(member_id, None)
        await self.broadcast(code, {"type": "member_left", "member_id": member_id})

    async def broadcast_trip_started(self, code: str):
        await self.broadcast(code, {"type": "trip_started"})

    async def broadcast_trip_ended(self, code: str):
        self.locations.pop(code, None)
        await self.broadcast(code, {"type": "trip_ended"})

    async def update_location(self, code: str, member_id: str, location: dict):
        payload = {"member_id": member_id, **location}
        self.locations.setdefault(code, {})[member_id] = payload
        await self.broadcast(code, {"type": "location_updated", "location": payload})


manager = ConnectionManager()
bearer = HTTPBearer()
JWT_SECRET = os.getenv("JWT_SECRET", "development-only-change-me")


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 600_000)
    return f"pbkdf2_sha256$600000${base64.b64encode(salt).decode()}${base64.b64encode(digest).decode()}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, iterations, salt_text, digest_text = encoded.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        salt = base64.b64decode(salt_text)
        expected = base64.b64decode(digest_text)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, int(iterations))
        return hmac.compare_digest(actual, expected)
    except (ValueError, TypeError):
        return False


def serialize_user(user: User):
    return {
        "id": user.id,
        "username": user.name,
        "name": user.name,
        "phoneNumber": user.phone_number,
        "email": user.email,
        "carName": user.car_name,
        "carNumber": user.car_number,
    }


def create_token(user: User):
    expires = datetime.now(timezone.utc) + timedelta(days=30)
    return jwt.encode({"sub": str(user.id), "exp": expires}, JWT_SECRET, algorithm="HS256")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer), db: Session = Depends(get_db)) -> User:
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=["HS256"])
        user = db.get(User, int(payload["sub"]))
    except (jwt.PyJWTError, KeyError, TypeError, ValueError):
        user = None
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid or expired login")
    return user


def serialize(convoy: Convoy):
    creator_name = next((m.name for m in convoy.members if m.member_id == convoy.creator_id), None)
    return {
        "code": convoy.code,
        "creator_id": convoy.creator_id,
        "creator_name": creator_name,
        "name": convoy.name,
        "destination": convoy.destination,
        "started": convoy.started,
        "members": [
            {
                "member_id": member.member_id,
                "name": member.name,
                "car_name": member.car_name,
                "car_number": member.car_number,
            }
            for member in convoy.members
        ],
    }


def get_convoy_or_404(db: Session, code: str) -> Convoy:
    convoy = db.scalar(select(Convoy).where(Convoy.code == code.upper()))
    if convoy is None:
        raise HTTPException(status_code=404, detail="Convoy code not found")
    return convoy


def make_code(db: Session):
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(20):
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if db.get(Convoy, code) is None:
            return code
    raise HTTPException(status_code=503, detail="Could not create a unique room code")


def migrate_schema():
    Base.metadata.create_all(engine)
    if engine.dialect.name != "sqlite":
        return
    with engine.connect() as conn:
        columns = {row[1] for row in conn.execute(text("PRAGMA table_info(convoys)")).fetchall()}
        if "name" not in columns:
            conn.execute(text("ALTER TABLE convoys ADD COLUMN name VARCHAR(120) NOT NULL DEFAULT 'Untitled Trip'"))
        if "destination" not in columns:
            conn.execute(text("ALTER TABLE convoys ADD COLUMN destination VARCHAR(255)"))
        conn.execute(text("UPDATE convoys SET name = 'Trip ' || code WHERE name = 'Untitled Trip' OR name IS NULL OR name = ''"))
        conn.commit()


app = FastAPI(title="Carkie Convoy API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in os.getenv("CORS_ORIGINS", "*").split(",")],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def create_tables():
    migrate_schema()


@app.get("/health")
def health():
    return {"status": "ok", "gps_protocol": 1}


@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
def register(body: RegisterInput, db: Session = Depends(get_db)):
    email = body.email.strip().lower()
    if db.scalar(select(User).where(User.email == email)) is not None:
        raise HTTPException(status_code=409, detail="An account with this email already exists")
    user = User(
        email=email,
        password_hash=hash_password(body.password),
        name=body.name.strip(),
        phone_number=body.phone_number.strip(),
        car_name=body.car_name.strip(),
        car_number=body.car_number.strip().upper(),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"token": create_token(user), "user": serialize_user(user)}


@app.post("/auth/login")
def login(body: LoginInput, db: Session = Depends(get_db)):
    user = db.scalar(select(User).where(User.email == body.email.strip().lower()))
    if user is None or not verify_password(body.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Incorrect email or password")
    return {"token": create_token(user), "user": serialize_user(user)}


@app.get("/profile")
def get_profile(user: User = Depends(current_user)):
    return serialize_user(user)


@app.patch("/profile")
def update_profile(body: ProfileUpdateInput, user: User = Depends(current_user), db: Session = Depends(get_db)):
    email = body.email.strip().lower()
    duplicate = db.scalar(select(User).where(User.email == email, User.id != user.id))
    if duplicate is not None:
        raise HTTPException(status_code=409, detail="An account with this email already exists")
    user.email = email
    user.name = body.name.strip()
    user.phone_number = body.phone_number.strip()
    user.car_name = body.car_name.strip()
    user.car_number = body.car_number.strip().upper()
    db.commit()
    db.refresh(user)
    return serialize_user(user)


@app.post("/convoys", status_code=status.HTTP_201_CREATED)
def create_convoy(body: CreateConvoyInput, db: Session = Depends(get_db)):
    code = make_code(db)
    member_data = body.model_dump()
    convoy = Convoy(code=code, creator_id=body.member_id, name=f"{body.name.strip()}'s Convoy")
    convoy.members.append(ConvoyMember(convoy_code=code, **member_data))
    db.add(convoy)
    db.commit()
    db.refresh(convoy)
    return serialize(convoy)


@app.post("/convoys/{code}/join")
def join_convoy(code: str, member: MemberInput, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    convoy = get_convoy_or_404(db, code)
    existing = db.get(ConvoyMember, (convoy.code, member.member_id))
    if existing is None:
        db.add(ConvoyMember(convoy_code=convoy.code, **member.model_dump()))
        db.commit()
    db.refresh(convoy)
    room_data = serialize(convoy)
    background_tasks.add_task(manager.broadcast_room_updated, convoy.code, room_data)
    return room_data


@app.get("/convoys/{code}")
def get_convoy(code: str, db: Session = Depends(get_db)):
    return serialize(get_convoy_or_404(db, code))


@app.post("/convoys/{code}/start")
def start_convoy(code: str, body: StartInput, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    convoy = get_convoy_or_404(db, code)
    if convoy.creator_id != body.member_id:
        raise HTTPException(status_code=403, detail="Only the convoy creator can start the trip")
    convoy.started = True
    db.commit()
    db.refresh(convoy)
    room_data = serialize(convoy)
    background_tasks.add_task(manager.broadcast_room_updated, convoy.code, room_data)
    background_tasks.add_task(manager.broadcast_trip_started, convoy.code)
    return room_data


@app.post("/convoys/{code}/leave")
def leave_convoy(code: str, body: MemberActionInput, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    convoy = get_convoy_or_404(db, code)
    member = db.get(ConvoyMember, (convoy.code, body.member_id))
    if member is None:
        raise HTTPException(status_code=404, detail="Member not found in this convoy")

    is_creator = convoy.creator_id == body.member_id
    convoy_code = convoy.code
    db.delete(member)
    db.commit()

    if is_creator:
        remaining = db.scalar(select(Convoy).where(Convoy.code == convoy_code))
        if remaining:
            db.delete(remaining)
            db.commit()
        background_tasks.add_task(manager.broadcast_trip_ended, convoy_code)
        return {"deleted": True}

    convoy = get_convoy_or_404(db, convoy_code)
    room_data = serialize(convoy)
    background_tasks.add_task(manager.broadcast_member_left, convoy_code, body.member_id)
    background_tasks.add_task(manager.broadcast_room_updated, convoy_code, room_data)
    return room_data


@app.post("/convoys/{code}/end")
def end_convoy(code: str, body: MemberActionInput, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    convoy = get_convoy_or_404(db, code)
    if convoy.creator_id != body.member_id:
        raise HTTPException(status_code=403, detail="Only the convoy leader can end the trip")

    convoy_code = convoy.code
    db.delete(convoy)
    db.commit()
    background_tasks.add_task(manager.broadcast_trip_ended, convoy_code)
    return {"ended": True}


@app.delete("/convoys/{code}/members/{target_member_id}")
def kick_member(code: str, target_member_id: str, body: MemberActionInput, db: Session = Depends(get_db)):
    convoy = get_convoy_or_404(db, code)
    if convoy.creator_id != body.member_id:
        raise HTTPException(status_code=403, detail="Only the convoy creator can kick members")
    if convoy.started:
        raise HTTPException(status_code=409, detail="Cannot kick members after the trip has started")
    if target_member_id == convoy.creator_id:
        raise HTTPException(status_code=400, detail="Cannot kick the convoy creator")

    member = db.get(ConvoyMember, (convoy.code, target_member_id))
    if member is None:
        raise HTTPException(status_code=404, detail="Member not found in this convoy")

    db.delete(member)
    db.commit()
    db.refresh(convoy)
    room_data = serialize(convoy)
    asyncio.create_task(manager.broadcast_member_kicked(convoy.code, target_member_id))
    asyncio.create_task(manager.broadcast_room_updated(convoy.code, room_data))
    return room_data


@app.patch("/convoys/{code}")
def update_convoy(code: str, body: UpdateConvoyInput, db: Session = Depends(get_db)):
    convoy = get_convoy_or_404(db, code)
    if convoy.creator_id != body.member_id:
        raise HTTPException(status_code=403, detail="Only the convoy creator can update trip settings")

    if body.name is not None:
        convoy.name = body.name.strip()
    if body.destination is not None:
        destination = body.destination.strip()
        convoy.destination = destination or None

    db.commit()
    db.refresh(convoy)
    room_data = serialize(convoy)
    asyncio.create_task(manager.broadcast_room_updated(convoy.code, room_data))
    return room_data


@app.websocket("/convoys/{code}/ws")
async def convoy_websocket(websocket: WebSocket, code: str, member_id: str = Query(...)):
    code = code.upper()
    db = SessionLocal()
    try:
        convoy = db.scalar(select(Convoy).where(Convoy.code == code))
        if convoy is None:
            await websocket.close(code=1008)
            return
        member = db.get(ConvoyMember, (code, member_id))
        if member is None:
            await websocket.close(code=1008)
            return
    finally:
        db.close()

    await manager.connect(code, member_id, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue
            try:
                message = json.loads(data)
                if message.get("type") != "location_update":
                    continue
                latitude = float(message["latitude"])
                longitude = float(message["longitude"])
                if not math.isfinite(latitude) or not math.isfinite(longitude) or not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
                    continue
                speed = message.get("speed")
                heading = message.get("heading")
                await manager.update_location(code, member_id, {
                    "lat": latitude,
                    "long": longitude,
                    "spe": float(speed) if speed is not None and math.isfinite(float(speed)) else None,
                    "h": float(heading) if heading is not None and math.isfinite(float(heading)) else None,
                    "time": datetime.now(timezone.utc).isoformat(),
                })
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
    except WebSocketDisconnect:
        manager.disconnect(code, member_id)
        await manager.broadcast_presence(code)
