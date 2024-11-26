from mrjob.job import MRJob

class ProductConversionRate(MRJob):

    def mapper(self, _, line):
        fields = line.strip().split(",")
        if len(fields) == 5:  # user_activity.csv
            product_id = fields[3]
            yield product_id, "interaction"
        elif len(fields) == 4:  # transactions.csv
            product_id = fields[2]
            yield product_id, "purchase"

    def reducer(self, product_id, interactions_purchases):
        interactions = 0
        purchases = 0
        for record in interactions_purchases:
            if record == "interaction":
                interactions += 1
            elif record == "purchase":
                purchases += 1
        if interactions > 0:
            conversion_rate = (purchases / interactions) * 100
            yield product_id, conversion_rate
        else:
            yield product_id, 0.0

if __name__ == '__main__':
    ProductConversionRate.run()
