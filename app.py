import os
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Date, Enum, Numeric, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
from typing import Optional
from datetime import date
from datetime import datetime
from sqlalchemy.orm import aliased
from globals import arbitrage_sides_lookup

# Load environment variables from .env
load_dotenv()

# Database Setup
DATABASE_URL = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Initialize FastAPI app
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Update this to your frontend's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# SQLAlchemy Models
class BetDescription(Base):
    __tablename__ = 'bet_description'
    bet_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    expiration_date = Column(Date, nullable=False)
    website = Column(String(255))
    status = Column(Enum('open', 'closed'))
    is_arbitrage = Column(Enum('yes', 'no'))

class ArbitrageOpportunities(Base):
    __tablename__ = 'arbitrage_opportunities'
    arb_id = Column(Integer, primary_key=True, index=True)
    bet_id1 = Column(Integer, ForeignKey('bet_description.bet_id'))
    bet_id2 = Column(Integer, ForeignKey('bet_description.bet_id'))
    timestamp = Column(DateTime)
    profit = Column(Numeric)

class SimilarEventOptions(Base):
    __tablename__ = 'similar_event_options'
    event_id = Column(Integer, primary_key=True, index=True)
    option_id_1 = Column(Integer)
    option_id_2 = Column(Integer)
    option_name_1 = Column(String(255))
    option_name_2 = Column(String(255))

class SimilarEvents(Base):
    __tablename__ = 'similar_events'
    event_id = Column(Integer, primary_key=True, index=True)
    bet_id_1 = Column(Integer)
    description_1 = Column(String(255))
    website_1 = Column(String(255))
    bet_id_2 = Column(Integer)
    description_2 = Column(String(255))
    website_2 = Column(String(255))

class BetSides(Base):
    __tablename__ = 'arbitrage_bet_sides'
    arb_id = Column(Integer, ForeignKey('arbitrage_opportunities.arb_id'), primary_key=True)
    bet_side_1 = Column(String(10), nullable=False)
    bet_side_2 = Column(String(10), nullable=False)

# Create all tables in the database
Base.metadata.create_all(bind=engine)

# Pydantic Models (for validation and serialization)
class BetDescriptionBase(BaseModel):
    name: str
    expiration_date: date
    website: Optional[str] = None
    status: Optional[str] = None
    is_arbitrage: Optional[str] = None

class BetDescriptionCreate(BetDescriptionBase):
    pass

class BetDescriptionResponse(BetDescriptionBase):
    bet_id: int

    class Config:
        orm_mode = True

# Dependency to get database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# CRUD Operations for Bet Description

@app.get("/api/v1/bets", response_model=list[BetDescriptionResponse])
def get_bets(db: Session = Depends(get_db)):
    bets = db.query(BetDescription).all()
    return bets

@app.get("/api/v1/bets/{bet_id}", response_model=BetDescriptionResponse)
def get_bet(bet_id: int, db: Session = Depends(get_db)):
    bet = db.query(BetDescription).filter(BetDescription.bet_id == bet_id).first()
    if not bet:
        raise HTTPException(status_code=404, detail="Bet not found")
    return bet

@app.post("/api/v1/bets", response_model=BetDescriptionResponse)
def create_bet(bet: BetDescriptionCreate, db: Session = Depends(get_db)):
    db_bet = BetDescription(
        name=bet.name,
        expiration_date=bet.expiration_date,
        website=bet.website,
        status=bet.status,
        is_arbitrage=bet.is_arbitrage
    )
    db.add(db_bet)
    db.commit()
    db.refresh(db_bet)
    return db_bet

@app.put("/api/v1/bets/{bet_id}", response_model=BetDescriptionResponse)
def update_bet(bet_id: int, bet: BetDescriptionCreate, db: Session = Depends(get_db)):
    db_bet = db.query(BetDescription).filter(BetDescription.bet_id == bet_id).first()
    if not db_bet:
        raise HTTPException(status_code=404, detail="Bet not found")
    
    db_bet.name = bet.name
    db_bet.expiration_date = bet.expiration_date
    db_bet.website = bet.website
    db_bet.status = bet.status
    db_bet.is_arbitrage = bet.is_arbitrage
    db.commit()
    return db_bet

@app.delete("/api/v1/bets/{bet_id}", response_model=dict)
def delete_bet(bet_id: int, db: Session = Depends(get_db)):
    db_bet = db.query(BetDescription).filter(BetDescription.bet_id == bet_id).first()
    if not db_bet:
        raise HTTPException(status_code=404, detail="Bet not found")
    
    db.delete(db_bet)
    db.commit()
    return {"message": "Bet deleted"}

# CRUD Operations for Arbitrage Opportunities

class ArbitrageOpportunitiesDetailResponse(BaseModel):
    arb_id: int
    bet_id1: int
    bet_id2: int
    bet_description_1: str
    bet_description_2: str
    website_1: str
    website_2: str
    option_name_1: str
    option_name_2: str
    bet_side_1: str
    bet_side_2: str
    profit: float
    timestamp: Optional[str]

    class Config:
        orm_mode = True

@app.get("/api/v1/arbitrage", response_model=list[ArbitrageOpportunitiesDetailResponse])
def get_all_arbitrage_opportunities(db: Session = Depends(get_db)):
    opportunities = db.query(ArbitrageOpportunities).all()

    if not opportunities:
        raise HTTPException(status_code=404, detail="No arbitrage opportunities found")

    # Create the response list
    results = []
    for opportunity in opportunities:
        # Fetching bet descriptions
        bet1 = db.query(BetDescription).filter(BetDescription.bet_id == opportunity.bet_id1).first()
        bet2 = db.query(BetDescription).filter(BetDescription.bet_id == opportunity.bet_id2).first()

        # Handling None values for bets
        bet_description_1 = bet1.name if bet1 else "N/A"
        bet_description_2 = bet2.name if bet2 else "N/A"
        website_1 = bet1.website if bet1 else "N/A"
        website_2 = bet2.website if bet2 else "N/A"

        # Fetching event_id from similar_events table
        similar_event = db.query(SimilarEvents).filter(
            (SimilarEvents.bet_id_1 == opportunity.bet_id1) &
            (SimilarEvents.bet_id_2 == opportunity.bet_id2)
        ).first()

        # Use event_id to fetch data from similar_event_options table
        if similar_event:
            similar_event_options = db.query(SimilarEventOptions).filter(
                SimilarEventOptions.event_id == similar_event.event_id
            ).first()
        else:
            similar_event_options = None

        # Handling None values for similar_event_options
        option_name_1 = similar_event_options.option_name_1 if similar_event_options else "N/A"
        option_name_2 = similar_event_options.option_name_2 if similar_event_options else "N/A"

        # Fetching bet sides from the arbitrage_bet_sides table
        bet_sides = db.query(BetSides).filter(BetSides.arb_id == opportunity.arb_id).first()

        # Handling None values for bet_sides
        if bet_sides:
            bet_side_1 = bet_sides.bet_side_1
            bet_side_2 = bet_sides.bet_side_2
        else:
            bet_side_1 = "Unknown"  # Fallback if no bet sides found
            bet_side_2 = "Unknown"  # Fallback if no bet sides found

        # Building the result dictionary
        result = {
            "arb_id": opportunity.arb_id,
            "bet_id1": opportunity.bet_id1,
            "bet_id2": opportunity.bet_id2,
            "bet_description_1": bet_description_1,
            "bet_description_2": bet_description_2,
            "website_1": website_1,
            "website_2": website_2,
            "option_name_1": option_name_1,
            "option_name_2": option_name_2,
            "bet_side_1": bet_side_1,
            "bet_side_2": bet_side_2,
            "profit": float(opportunity.profit) if opportunity.profit is not None else 0.0,
            "timestamp": opportunity.timestamp.isoformat() if opportunity.timestamp else "N/A",
        }
        results.append(result)

    return results

@app.get("/api/v1/arbitrage/{arb_id}", response_model=ArbitrageOpportunitiesDetailResponse)
def get_arbitrage_opportunity(arb_id: int, db: Session = Depends(get_db)):
    # Fetch the arbitrage opportunity
    opportunity = db.query(ArbitrageOpportunities).filter(ArbitrageOpportunities.arb_id == arb_id).first()

    if not opportunity:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    # Fetch bet descriptions
    bet1 = db.query(BetDescription).filter(BetDescription.bet_id == opportunity.bet_id1).first()
    bet2 = db.query(BetDescription).filter(BetDescription.bet_id == opportunity.bet_id2).first()

    # Handling None values for bets
    bet_description_1 = bet1.name if bet1 else "N/A"
    bet_description_2 = bet2.name if bet2 else "N/A"
    website_1 = bet1.website if bet1 else "N/A"
    website_2 = bet2.website if bet2 else "N/A"

    # Fetching event_id from similar_events table
    similar_event = db.query(SimilarEvents).filter(
        (SimilarEvents.bet_id_1 == opportunity.bet_id1) &
        (SimilarEvents.bet_id_2 == opportunity.bet_id2)
    ).first()

    # Use event_id to fetch data from similar_event_options table
    if similar_event:
        similar_event_options = db.query(SimilarEventOptions).filter(
            SimilarEventOptions.event_id == similar_event.event_id
        ).first()
    else:
        similar_event_options = None

    # Handling None values for similar_event_options
    option_name_1 = similar_event_options.option_name_1 if similar_event_options else "N/A"
    option_name_2 = similar_event_options.option_name_2 if similar_event_options else "N/A"

    # Fetching bet sides from the arbitrage_bet_sides table
    bet_sides = db.query(BetSides).filter(BetSides.arb_id == opportunity.arb_id).first()

    # Handling None values for bet_sides
    if bet_sides:
        bet_side_1 = bet_sides.bet_side_1
        bet_side_2 = bet_sides.bet_side_2
    else:
        bet_side_1 = "Unknown"  # Fallback if no bet sides found
        bet_side_2 = "Unknown"  # Fallback if no bet sides found

    # Build the result dictionary
    result = {
        "arb_id": opportunity.arb_id,
        "bet_id1": opportunity.bet_id1,
        "bet_id2": opportunity.bet_id2,
        "bet_description_1": bet_description_1,
        "bet_description_2": bet_description_2,
        "website_1": website_1,
        "website_2": website_2,
        "option_name_1": option_name_1,
        "option_name_2": option_name_2,
        "bet_side_1": bet_side_1,
        "bet_side_2": bet_side_2,
        "profit": float(opportunity.profit) if opportunity.profit is not None else 0.0,
        "timestamp": opportunity.timestamp.isoformat() if opportunity.timestamp else "N/A",
    }

    return result

@app.post("/api/v1/arbitrage", response_model=ArbitrageOpportunitiesDetailResponse)
def create_arbitrage_opportunity(opportunity: ArbitrageOpportunitiesDetailResponse, db: Session = Depends(get_db)):
    print(f"Incoming request to create arbitrage opportunity: {opportunity.dict()}")
    try:
        db_opportunity = ArbitrageOpportunities(
            bet_id1=opportunity.bet_id1,
            bet_id2=opportunity.bet_id2,
            timestamp=datetime.fromisoformat(opportunity.timestamp) if opportunity.timestamp else None,
            profit=opportunity.profit
        )
        db.add(db_opportunity)
        db.commit()
        db.refresh(db_opportunity)
        print(f"Arbitrage opportunity successfully created with ID: {db_opportunity.arb_id}")
        return db_opportunity
    except Exception as e:
        print(f"Failed to create arbitrage opportunity: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.put("/api/v1/arbitrage/{arb_id}", response_model=ArbitrageOpportunitiesDetailResponse)
def update_arbitrage_opportunity(arb_id: int, opportunity: ArbitrageOpportunitiesDetailResponse, db: Session = Depends(get_db)):
    db_opportunity = db.query(ArbitrageOpportunities).filter(ArbitrageOpportunities.arb_id == arb_id).first()
    if not db_opportunity:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    
    db_opportunity.bet_id1 = opportunity.bet_id1
    db_opportunity.bet_id2 = opportunity.bet_id2
    db_opportunity.timestamp = opportunity.timestamp
    db_opportunity.profit = opportunity.profit
    db.commit()
    return db_opportunity

@app.delete("/api/v1/arbitrage/{arb_id}", response_model=dict)
def delete_arbitrage_opportunity(arb_id: int, db: Session = Depends(get_db)):
    db_opportunity = db.query(ArbitrageOpportunities).filter(ArbitrageOpportunities.arb_id == arb_id).first()
    if not db_opportunity:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    
    db.delete(db_opportunity)
    db.commit()
    return {"message": "Opportunity deleted"}

# Main
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=9000, reload=True)

# Additional SQLAlchemy Model for Price Table
class Price(Base):
    __tablename__ = 'price'
    option_id = Column(Integer, ForeignKey('bet_choice.option_id'), primary_key=True, index=True)
    timestamp = Column(Date, primary_key=True)
    volume = Column(Numeric, nullable=True)
    yes_price = Column(Numeric, nullable=True)
    no_price = Column(Numeric, nullable=True)
    yes_odds = Column(Numeric, nullable=True)
    no_odds = Column(Numeric, nullable=True)

# Pydantic Models for Price Table
class PriceBase(BaseModel):
    option_id: int
    timestamp: date
    volume: Optional[float] = None
    yes_price: Optional[float] = None
    no_price: Optional[float] = None
    yes_odds: Optional[float] = None
    no_odds: Optional[float] = None

class PriceCreate(PriceBase):
    pass

class PriceResponse(PriceBase):
    class Config:
        orm_mode = True

# CRUD Operations for Price Table

@app.get("/api/v1/prices", response_model=list[PriceResponse])
def get_prices(db: Session = Depends(get_db)):
    prices = db.query(Price).all()
    return prices

@app.get("/api/v1/prices/{option_id}/{timestamp}", response_model=PriceResponse)
def get_price(option_id: int, timestamp: date, db: Session = Depends(get_db)):
    price = db.query(Price).filter(Price.option_id == option_id, Price.timestamp == timestamp).first()
    if not price:
        raise HTTPException(status_code=404, detail="Price not found")
    return price

@app.post("/api/v1/prices", response_model=PriceResponse)
def create_price(price: PriceCreate, db: Session = Depends(get_db)):
    db_price = Price(
        option_id=price.option_id,
        timestamp=price.timestamp,
        volume=price.volume,
        yes_price=price.yes_price,
        no_price=price.no_price,
        yes_odds=price.yes_odds,
        no_odds=price.no_odds
    )
    db.add(db_price)
    db.commit()
    db.refresh(db_price)
    return db_price

@app.put("/api/v1/prices/{option_id}/{timestamp}", response_model=PriceResponse)
def update_price(option_id: int, timestamp: date, price: PriceCreate, db: Session = Depends(get_db)):
    db_price = db.query(Price).filter(Price.option_id == option_id, Price.timestamp == timestamp).first()
    if not db_price:
        raise HTTPException(status_code=404, detail="Price not found")
    
    db_price.volume = price.volume
    db_price.yes_price = price.yes_price
    db_price.no_price = price.no_price
    db_price.yes_odds = price.yes_odds
    db_price.no_odds = price.no_odds
    db.commit()
    return db_price

@app.delete("/api/v1/prices/{option_id}/{timestamp}", response_model=dict)
def delete_price(option_id: int, timestamp: date, db: Session = Depends(get_db)):
    db_price = db.query(Price).filter(Price.option_id == option_id, Price.timestamp == timestamp).first()
    if not db_price:
        raise HTTPException(status_code=404, detail="Price not found")
    
    db.delete(db_price)
    db.commit()
    return {"message": "Price deleted"}
