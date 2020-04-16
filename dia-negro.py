from mrjob.job import MRJob, MRStep

class MRBlackDay(MRJob):

    def mapper(self, _, line):      
        company, price, date = line.split(",")  
        try:
            price = float(price) 
            yield company, (price, date)
        except: pass  

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_minDate),
            MRStep(reducer=self.reducer_values),
            MRStep(reducer=self.reducer_blackDay)
              ]
        
                
    def reducer_minDate(self, key, values):
        minPrice, minDate = next(values)
        for price, date in values:
            if price < minPrice:
                minPrice, minDate = price, date
         
        yield minDate, 1

    def reducer_values(self, minDate, values):
 
        yield "date", (minDate, sum(values))

    #calculating the blackday
    def reducer_blackDay(self, date, values):
        blackDay, actualValue = next(values)
        for date, value in values:
            if value > actualValue:
                blackDay = date
        
        yield "BlacDay", blackDay

    

if __name__ == '__main__':
    MRBlackDay.run()