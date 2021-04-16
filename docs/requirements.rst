Project Requirements
====================

A recommender system the tells you the location of your nearest safe parking spot. Definition of 
"safe" in this context - lesser probability of your car being towed. 
We can also recommend cluster of parking spots. We might return the top 5 clusters based on some 
statistic.

Input is a geocoordiante location and the output is a set of parking spots in close vicinity of
that parking spot. We will have the dask dataframe the contains all the necessary columns.
The steps will be as follows - 
1. Take a geo coordinate as an input.
2. Using the processed data frame, add a new column that stores the Euclidean distance of the input point from
   all other points. 
3. Next, sort the dataframe using values from the new column to find N closest parking spots.
4. Next, sort the dataframe based on the probability column.
5. If needed by the user, we can sort by the price column.

We take the past data - this data tell us at what locations cars were towed in the past.
From 2000-2015 we have the pickup location and the timestamp. From 2015-2020 we have pickup and
drop off location along with both the timestamp.


Sarting with the list of user stories -
* As a user find location of top10 closest parking spots, given the location
* Sort parking spot by distance and price
* Colour code the sorted results
* Using goe spatial data of the montreal metro, suggest user to use a metro becuase of 
  parking restrictions at the destination.


Libraries Used
==============
* Pandas
* Numpy
* Folium
* Scikit-Learn
* GMPlot

Misc Notes
==========
# parking_spot can be a class that contains data about the spot.
# what is the best way of manpulating/storing coordinates
# https://www.geeksforgeeks.org/python-plotting-google-map-using-gmplot-package/