from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import time
import itertools

# Par de peliculas por usuario.
# Promedio de ese par de peliculas por usuario.
# Promedio de promedio de ese par de peliculas para todos los usuarios que vieron ese par de peliculas.
# Max y min de ese promedio de promedio.

class MRWordFrequencyCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, record):
        yield [record['userId'], [record['movieId'], record['rating']]]

    def reducer(self, key, values):
        ratings = []
        movies_ids = []
        movie = {}
        for value in values:
            ratings.append(value[1])
            movies_ids.append(value[0])
        for i in xrange(0, len(movies_ids)):
            movie[movies_ids[i]] = ratings[i]
        combinations = list(itertools.combinations(movies_ids, 2))
        for combination in combinations:
            average_pair_movies = ((movie[combination[0]] + movie[combination[1]])/2)
            yield [combination, average_pair_movies]

    def reducer2(self, key, values):
        averages = []
        amount_averages = 0
        for value in values:
            averages.append(value)
            amount_averages += 1
        average_of_average = float(sum(averages)/amount_averages)
        yield [key, average_of_average]

    def mapper2(self, key, record):
        yield ["MAX", [record, key]]
        yield ["MIN", [record, key]]

    def reducer3(self, key, values):
        combinations = []
        average_of_averages = []
        pair_movies = {}
        for value in values:
            combinations.append(value[1])
            average_of_averages.append(value[0])
        for i in xrange(0, len(combinations)):
            pair_movies[str(combinations[i])] = average_of_averages[i]
        max_average = max(average_of_averages)
        min_average = min(average_of_averages)
        max_average_combinations = []
        min_average_combinations = []
        for combination in combinations:
            if pair_movies[str(combination)] == max_average:
                max_average_combinations.append(combination)
            elif pair_movies[str(combination)] == min_average:
                min_average_combinations.append(combination)
        if key == "MAX":
            yield [key, [max_average, max_average_combinations]]
        else:
            yield [key, [min_average, min_average_combinations]]

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer),
                MRStep(reducer=self.reducer2),
                MRStep(mapper=self.mapper2, reducer=self.reducer3)]

if __name__ == '__main__':
    time_start = time.clock()
    MRWordFrequencyCount.run()
    time_end = time.clock()
    print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)
