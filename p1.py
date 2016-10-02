from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import time
import itertools

class MRWordFrequencyCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_user_movies(self, _, record):
        yield [record['userId'], record['movieId']]

    def reducer(self, key, values):
        combinations = list(itertools.combinations(values, 2))
        for combination in combinations:
            yield [combination, 1]

    def reducer2(self, key, values):
        yield [key, sum(values)]

    def mapper2(self, key, values):
        yield ["MAX", [values, key]]

    def reducer3(self, key, values):
        yield [key, max(values)]

    def steps(self):
        return [MRStep(mapper=self.mapper_user_movies, reducer=self.reducer), MRStep(reducer=self.reducer2),
                MRStep(mapper=self.mapper2, reducer=self.reducer3)]

if __name__ == '__main__':
    time_start = time.clock()
    MRWordFrequencyCount.run()
    time_end = time.clock()
    print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)
