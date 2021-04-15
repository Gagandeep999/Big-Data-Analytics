NoToW
==============================

<b>ABSTRACT</b><br>
Parking in Montreal is becoming even more challenging and there is always the risk of getting towed. We have acquired 5 years worth of data that includes a timestamp, geo-coordinate of the origin and destination of the tow and the reason for towing a car in Montreal. Our aim is to predict a real number indicative of the radius within which the probability of finding a towed car is maximum. We also aim to use the parking data of the City of Montreal and perform k-means clustering with k=18 which is the number of boroughs in Montreal to verify if a spot is classified as per its borough.

<br><b>INTRODUCTION</b><br>
<b>Context</b><br>
Montreal is the second-most populous city in Canada and most populous city in the province of Quebec. For over a century and a half, Montreal was the industrial and financial center of Canada because of which Montreal is amongst the top 10 famous cities of Canada [1]. Montreal attracts many local and international travelers and tourists all year round. Some like to travel with their own cars, while some like to rent cars while being in the city.<br>
<b>Objectives</b><br>
Our objective is to minimize the lag between the time a car is towed and the time the owner gets it back. In order to do so, we plan to use supervised learning to train a model that learns how far the cars were placed in the past so that it can predict how far the user can expect their car to be. For the second objective we will be implementing a k-means algorithm from scratch in dask and verify the results of the classification.<br>
<b>Presentation of the problem to solve</b><br>
Even though having a car makes it easy to travel around when you are traveling it can turn out to be a bummer if your car gets towed and you have to waste a lot of time getting it back. This experience can be far worse if you are traveling with toddlers. The problem that we want to address is related to the towing of cars. <br>
<b>Related work</b><br>
We plan to first of all clean, preprocess, and sanitize our data to make it readily consumable by the model. Next, we plan to do some feature engineering by combining this towing data with the weather data on the datetime column. For the supervised learning, the last part would be to train a linear regression model that predicts the radius of the towing destination. In terms of the k-means, we plan to use the standard k-means algorithm but implement each and every step in dask.


<b>MATERIALS AND METHODS</b><br>
<b>Datasets</b><br>
The dataset was obtained from the Govt of Canada open data portal website. It contains information from the year 2015-2020 with almost ~250,000 rows of data. We used the following features from the dataset -  date_origine, longitude_origine, latitude_origine, longitude_destination, latitude_destination,  motif_remorquage. 

Our second dataset that wasused for the supervised learning task was the weather data. This was also from the Govt of Canada website but it's available in a yearly format. So, we have 6 files for years ranging from 2015-2020 with 366 rows for each year with the exception of 2016 and 2020 which are leap years. It contains observations made at the Montreal/Pierre Elliott Trudeau Intl weather station. The list of features used are - Longitude (x), Latitude (y), Date/Time, Month, Day, Mean Temp (°C), Total Rain (mm), Total Snow (cm), Total Precip (mm).

We decided to use the data from the City of Montreal for the k-means clustering. It contains the details about all the parking spots on the island of Montreal. This data has 18635 rows one for each of the parking spot in Montreal and the data related to each spot was already been divided into multiple files as a relational database model. Some of the key features used - Snoplace, Nlongitude, Nlatitude.

<b>Technologies</b><br>
We wanted to leverage the methods learned in class to speed up our data preprocessing and cleaning process. The first task was to load data into a pyspark dataframe. From there on we were to perform all kinds of operations and benefit from the speed up of parallel processing. The most tricky part was to find the distance between the origin and destination of towing. The fact that we had geo-cordinates, we wanted to make sure that we take into consideration the spherical shape of the Earth while calculating the distance. In order to do that we used the Haversine Formula[Hav], which requires some series of manipulation on data to get the result. This series of manipulation was done using pyspark. Once that was complete, we combined this cleaned and processed towing data with the weather data.
As mentioned before, k-means clustering was implemented from scratch using dask. Our expectation was that if we choose the number of cluster exactly same as the number of boroughs in montreal, our k-means should classify each parking spot to it's own borough. 
<br>
<b>Algorithms</b><br>
Supervised Learning in the aforementioned paragraphs was acheieved using LinearRegression package available in the ML-Lib from Pyspark. By this time, we already had our data cleaned and ready to be fed to the model, but the input for this library was different from the regular scikit-learn libraries. Using another package from pyspark we merged all the feature columns into one vector for each row and converted the output/target_variable into a single valued vector. Using this input format we were able to train the model.
Using dask we read the Places.csv file that contained details related to all the parking spots. First step was to initilize the value of k that we got from our list of boroughs, next we randomly initialized entroids for each parking spot. Then we started the learning process, where for each iteration we would assign centroids to parking spots, calculate how many spots had their centroid changed (this is the stopping condition), depending on this number we would assign these newly calculated centroid to the already existing centroid in the dataframe and repeat the whole process until the number of spots that had their centroids chagned was reduced to zero.<br>

<b>RESULTS</b><br>
<b>Description of Dataset analysis</b><br>
Add more details here <br>
<b>Description of Implementation</b><br>
Add more details here<br>

<b>DISCUSSION</b><br>
<b>Discussion of solution</b><br>
Add more details here<br>
<b>Discussion of limitations</b><br>
Add more details here<br>
<b>Discussion of possible future work</b><br>
Add more details here<br>
<br>
___
<br>
<b>IMPLEMENTATION</b><br>
<b>Prerequisties</b><br>
Make sure you have anaconda installed on your system. Open anaconda prompt and run the following commands -

To setup the environment<br>
```$ conda create -n bigdata python=3.6```

Activate your environment<br>
```$ conda activate bigdata```

Naviagte to the project directory and run the command to installed the necessary libraries<br>
```$ pip install -r requirements.txt```
<br><br>

<b>Methodology</b><br>
Preprocesing scripts are located in the src/data directory. Navigate to the ```/src/data/``` folder and run the following command. Once it is complete, you have parquet files in the /data/interim/ directory.<br>
```$ python make_dataset.py ``` <br>

After the preprocessing, to build feature you can run the following command from the ```/src/features/``` directory. Once the execution finishes,
you can find the cleaned.data parquet file which is used for training the model, in /data/processed/ directory.<br>
```$ python build_features.py ``` <br>

Finally, to train the model the following commad from ```/src/models/``` directory. This will also save the trained model 
in the /models/\<today's-date>/ directory<br>
```$ python train_model.py ```
<br><br>
___
<br>
<b>PROJECT ORGANIZATION</b>


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


===========

[1] = https://en.wikipedia.org/wiki/Montreal

[2] = https://www.wardsauto.com/news-analysis/how-long-you-wait-roadside-service-depends-what-youre-calling

[Hav] = https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude

Extra content that can be used somewhere else

Using the acquired data we plan to train a linear regression model that learn how far the cars were placed in the past and predict the same for new unseen data.

From the mellow sounds of International Jazz Festival to the deafening formula race cars at the Grand Prix Monteal, from the florid orange sunrise at Mount Royal and to the spectacular sunset at Oratorie Saint-Joseph Montreal attracts tourists from all over the world all year round.

Imagine being a tourist and you have a schedule to visit top10 tourist locations of the city you are visiting, but all of a sudden you realize that your car is towed and which is not only going to cost extra on the budget for the trip but also time. You might have to skip a destination or two becuase of this inconvience.

A study found that cellular phone callers wait nearly as long, an average of 46mins before the help arrives[2].

Using the matplotlib we would next plot the towing of different years on a map of montreal and expect to see some trend realted to the towings.

such that our features include geocordinate, timestamp and weather condition at the time of towing
 
rue_origine, secteur_origine, arrondissement_origine, date_destination, rue_destination, secteur_destination, arrondissement_destination, 

Station Name, Climate ID, Data Quality, Max Temp (°C), Max Temp Flag, Min Temp (°C), Min Temp Flag, Mean Temp Flag, Heat Deg Days (°C), Heat Deg Days Flag, Cool Deg Days (°C), Cool Deg Days Flag, Total Rain Flag,  Total Snow Flag, Total Precip Flag,