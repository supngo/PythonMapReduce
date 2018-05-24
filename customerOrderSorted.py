from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCustomerOrdersSorted(MRJob):

  def steps(self):
    return [
      MRStep(mapper=self.mapper_get_amount, reducer=self.reducer_count_amount),
      MRStep(mapper=self.mapper_make_amount_key, reducer = self.reducer_sorted_amount)
    ]

  def mapper_get_amount(self, _, line):
    (customer, item, amount) = line.split(',')
    yield customer, float(amount)

  def reducer_count_amount(self, customers, amount):
    yield customers, sum(amount)
  
  def mapper_make_amount_key(self, customers, amount):
    yield '%04.02f'%float(amount), customers

  def reducer_sorted_amount(self, amount, customers):
    for customer in customers:
      yield customer, amount

if __name__ == '__main__':
  MRCustomerOrdersSorted.run()