from mrjob.job import MRJob

class RevenueCategoryJoin(MRJob):

    def mapper(self, _, line):
        fields = line.strip().split(",")
        if len(fields) == 3:  # products.csv
            product_id = fields[0]
            category = fields[2]
            yield product_id, ("category", category)
        elif len(fields) == 2:  # Revenue output from Step 1
            product_id = fields[0]
            revenue = float(fields[1])
            yield product_id, ("revenue", revenue)

    def reducer(self, product_id, records):
        category = None
        revenue = None
        for record_type, value in records:
            if record_type == "category":
                category = value
            elif record_type == "revenue":
                revenue = value
        if category and revenue is not None:
            yield category, (product_id, revenue)

if __name__ == '__main__':
    RevenueCategoryJoin.run()
