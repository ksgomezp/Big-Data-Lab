from mrjob.job import MRJob

class MREstables(MRJob):

    def mapper(self, _, line):      
        company, price, date = line.split(",")  
        try:
            price = float(price) 
            yield company, (price, date)
        except: pass  
        
                
    def reducer(self, key, values):
        establePrice, estableDate = next(values)
        result = {}
        estable = True
        for price, date in values:
            if price < establePrice:
                estable = False
        result["Company estable o creciendo"] = estable
        yield key, result

if __name__ == '__main__':
    MREstables.run()