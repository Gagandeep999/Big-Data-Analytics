NoToW
==============================

Parking in Montreal is becoming even more challenging and there is always the risk of getting towed. We have acquired 20 years worth of data that includes timestamp, geo coordinate, and reason for a Municipal Employee to request to tow a car in Montreal. We hope to analyze the data and use it to predict the demand for tow trucks depending on the time of year and location in an effort to better allocate tow trucks resources throughout Montreal. We also hope to calculate the probability of someone's car being towed based on their location and time of day.

CONTEXT - 
Montreal is the second-most populous city in Canada and most populous city in the province of Quebec. For over a century and a half, Montreal was the industrial and financial centre of Canada becuase of which Montreal is amongst the top 10 famous cities of Canada. From the mellow sounds of International Jazz Festival to the deafening formula race cars at the Grand Prix Monteal, from the florid orange sunrise at Mount Royal and to the spectacular sunset at Oratorie Saint-Joseph Montreal attracts tourists from all over the world all year round. Some like to travel with their own cars while some like to rent cars while being in the city. 
OBJECTIVE - 

PRESENTATION OF PROBLEM -
The problem that we want to address is the towing of cars. Imagine being a tourist and you have a schedule to visit top10 tourist locations of the city you are visiting, but all of a sudden you realize that your car is towed and which is not only going to cost extra on the budget for the trip but also time. You might have to skip a destination or two becuase of this inconvience
REALTED WORK - 
Firstly clean/preprocess the towing data. Plot it on a map of Montreal to have the first visualization. Then perform some statistical aggregation and plot those observations in hope of finding some patterns in the data. Next we match up the towing data with the parking data; expected work includes finding probability of each and every parking spot in Montreal so that we can have some basis of classification based on the probaility range.
Next piece of work is would be to have recommender system in place. Imagine entering the location address of the destination on google maps and then following the maps to get the location, as you arrive a good feature could be to have an idea of nearby parking spots. For this feature the current expected work is to have a system that accepts one geo coordinate location as the input and spits out a set of geo coordinate locations that are parking spots where the user might be able to park his car. The measure of "nearby" parking spots for the moment is eucledian distance.

DATASET - 
The dataset was obtained from the Govt of Canada open data portal website. This dataset is divided into two parts, first one contains the geo coordinates of the location from where the car was towed along with the timestamp. The second one contains the geo coordinates of the pickup and drop off location along with the time stamp. 
We also decided to use the data from the City of Montreal, which contains the details about all the parking spots. This data is available as csv files but it has already been divided as a relational database model. A key feature that we found interesting was the price of each and every parking spot.
TECHNOLOGY AND ALGORITHM - 
We plan to use dask for the recommendation of parking spots to make sure the results are processed fast and result is sent back to the user in "almost" real time. 
Another idea would be to take the top50 parking spots based on eucledian distance, then cluster those 50 spots and send back the cluster to the user, that way the user is not pointed to a specific spot, but a set of parking spots and depending on the availability parks his car.


Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable dataI dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
