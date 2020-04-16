from mrjob.job import MRJob

class MRMinMax(MRJob):

    def mapper(self, _, line):      
        company, price, date = line.split(",")  
        try:
            price = float(price) 
            yield company, (price, date)
        except: pass  
        
                
    def reducer(self, key, values):
        minPrice, minDate = next(values)
        maxPrice, maxDate = minPrice, minDate
        result = {}
        for price, date in values:
            if price < minPrice:
                minPrice, minDate = price, date
    
            if price > maxPrice:
                maxPrice, maxDate = price, date
            
        result["dia-menor-valor"] = (minDate,minPrice)
        result["dia-mayor-valor"] = (maxDate,maxPrice)
        yield key, result

if __name__ == '__main__':
    MRMinMax.run()