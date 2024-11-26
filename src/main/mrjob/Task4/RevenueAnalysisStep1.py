from mrjob.job import MRJob

class RevenueAnalysis(MRJob):

    def mapper(self, _, line):
        fields = line.strip().split(",")
        if len(fields) == 6:  # transactions.csv
            product_id = fields[3]
            revenue = float(fields[5])
            yield product_id, revenue

    def reducer(self, product_id, revenues):
        total_revenue = sum(revenues)
        yield product_id, total_revenue

if __name__ == '__main__':
    RevenueAnalysis.run()
