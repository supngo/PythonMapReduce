from mrjob.job import MRJob

class MRCustomerOrders(MRJob):

  def mapper(self, _, line):
    (customer, item, amount) = line.split(',')
    yield customer, float(amount)

  def reducer(self, customer, amount):
    yield customer, sum(amount)


if __name__ == '__main__':
  MRCustomerOrders.run()