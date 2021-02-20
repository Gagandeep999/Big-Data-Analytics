class ParkingSpot:
    def __init__(self,  name, latitude, longitue, price):
        self.name = name
        self.latitude = latitude
        self.longitue = longitue
        self.price = price

    def __str__(self):
        return ('Name: ', self.name, '\nLongitude: ', self.longitue, '\nLatitude: ', self.latitude, '\nPrice:', self.price)

    @property
    def _price(self):
        return self.price
    
    @_price.setter
    def _price(self, price):
        self.price = price

    @property
    def _coordinates(self):
        return (self.longitue, self.latitude)
    
    @_coordinates.setter
    def coordinates(self, latitude, longitude):
        self.latitude = latitude
        self.longitue = longitude
    