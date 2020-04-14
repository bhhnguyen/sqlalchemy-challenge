import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

import datetime as dt
from datetime import datetime

from flask import Flask, jsonify

#################################################
# Helper Functions
#################################################

# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(session, start_date, end_date=None):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        session: The session link from Python to the DB
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """

    if end_date is None:
        return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date)        

    else:
        return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br>"
        f"/api/v1.0/precipitation       -     Get precipitation data.<br>"
        f"/api/v1.0/stations            -     Get a list of station data.<br>"
        f"/api/v1.0/tobs                -     Get temperature data from most active station.<br>"
        f"/api/v1.0/[start]             -     Get temperature data after a specified date. FORMAT %Y-%m-%d<br>"
        f"/api/v1.0/[start]/[end]       -     Get temperature data in a date range. FORMAT %Y-%m-%d<br>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return all precipitation points in the dataset."""

    session = Session(engine)

    result = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    dateStr = result[0]
    last_date = datetime.strptime(dateStr,'%Y-%m-%d')

    year_ago = last_date - dt.timedelta(days=365)
    year_ago_str = datetime.strftime(year_ago,'%Y-%m-%d')

    # Perform a query to retrieve the data and precipitation scores
    query = (session
            .query(Measurement.date, Measurement.prcp)
            .filter(Measurement.date > year_ago_str))

    session.close()

    all_points = {}
    
    for date,prcp in query.all():
        all_points[date] = prcp

    return jsonify(all_points)


@app.route("/api/v1.0/stations")
def stations():
    """Return a list of stations."""

    session = Session(engine)

    results = session.query(Station.station,Station.name,Station.latitude,Station.longitude,Station.elevation).all()
    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    all_stations = []
    for station, name, lat, lng, elv in results:
        station_dict = {}
        station_dict['station'] = station
        station_dict['name'] = name
        station_dict['latitude'] = lat
        station_dict['longitude'] = lng
        station_dict['elevation'] = elv
        all_stations.append(station_dict)

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    """Return the temperature observations of the most active station in the last year.."""

    session = Session(engine)

    stationQuery = (session
                    .query(Measurement.station, func.count(Measurement.station))
                    .group_by(Measurement.station)
                    .order_by(func.count(Measurement.station).desc()))

    result = engine.execute(stationQuery.statement).fetchall()
    mostActiveStation = result[0][0]

    tempQuery = (session
                .query(Measurement.date,Measurement.tobs)
                .filter(Measurement.station == mostActiveStation))

    tempResults = engine.execute(tempQuery.statement).fetchall()
    session.close()

    all_temps = {}

    for date,tobs in tempResults:
        all_temps[date] = tobs

    return jsonify(all_temps)

@app.route("/api/v1.0/<start>")
def startTemps(start):
    """Fetch the min, max, and average temperatures after a start date."""

    session = Session(engine)
    tempData = calc_temps(session, start)
    session.close()
    tempDict = {}
    for tmin,tave,tmax in tempData:
        tempDict['TMIN'] = tmin
        tempDict['TAVE'] = tave
        tempDict['TMAX'] = tmax

    return jsonify(tempDict)

@app.route("/api/v1.0/<start>/<end>")
def startEndTemps(start, end):
    """Fetch the min, max, and average temperatures between a start and end date."""

    session = Session(engine)
    tempData = calc_temps(session, start, end)
    session.close()
    tempDict = {}
    for tmin,tave,tmax in tempData:
        tempDict['TMIN'] = tmin
        tempDict['TAVE'] = tave
        tempDict['TMAX'] = tmax

    return jsonify(tempDict)

if __name__ == '__main__':
    app.run(debug=True)
