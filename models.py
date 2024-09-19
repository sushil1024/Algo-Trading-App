from sqlalchemy import String, create_engine, select, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from log_config import get_logger

logger = get_logger(__name__)


class Base(DeclarativeBase):
    pass


class MarketData(Base):
    __tablename__ = "marketdata"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, nullable=False)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    open: Mapped[int] = mapped_column(String(50), nullable=False)
    high: Mapped[int] = mapped_column(String(50), nullable=False)
    low: Mapped[int] = mapped_column(String(50), nullable=False)
    close: Mapped[int] = mapped_column(String(50), nullable=False)
    timestamp: Mapped[DateTime] = mapped_column(String(50), nullable=False)
    data_type: Mapped[str] = mapped_column(String(50), nullable=False)


engine = create_engine('postgresql://postgres:r00t@localhost/fyers', echo=True)


# create main table
def create_table():
    Base.metadata.create_all(engine)


# select query
def read():
    temp = []
    session = Session(engine)
    stmt = select(MarketData)
    for data in session.scalars(stmt):
        temp.append([data.symbol, data.close, data.timestamp])
    return temp


# insert query
def insert_data(symbol, open, high, low, close, timestamp, data_type='1min'):
    with Session(engine) as session:
        data1 = MarketData(
            symbol=symbol,
            open=open,
            high=high,
            low=low,
            close=close,
            timestamp=timestamp,
            data_type=data_type
        )
        session.add_all([data1])
        session.commit()
        logger.info(f"Minute candle: O:{open}, H:{high}, L:{low}, C:{close}")
        return "data inserted successfully"


if __name__ == '__main__':
    create_table()

