from mrjob.job import MRJob

class TopProductsByRevenue(MRJob):

    def mapper(self, category, product_revenue):
        yield category, product_revenue

    def reducer(self, category, product_revenues):
        top_products = sorted(product_revenues, key=lambda x: x[1], reverse=True)[:3]
        for product_id, revenue in top_products:
            yield category, (product_id, revenue)

if __name__ == '__main__':
    TopProductsByRevenue.run()
