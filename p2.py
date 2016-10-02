from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import time
import itertools

class MRWordFrequencyCount(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def mapper_user_movies(self, _, record):
        yield [record['userId'], [record['movieId'], 1]]

    def reducer0(self, key, values):
        movie_ids = []
        amount_movies = []
        for value in values:
            movie_ids.append(value[0])
            amount_movies.append(value[1])
        ratings = sum(amount_movies)
        for movie_id in movie_ids:
            yield [[key, movie_id], ratings]

    def mapper(self, key, record):
        yield [key[1], [key[0], record]]

    def reducer(self, key, values):
        user_ids = []
        user_ratings = []
        for value in values:
            user_ids.append(value[0])
            user_ratings.append(value[1])
        user = {}
        for i in xrange(0, len(user_ids)):
            user[user_ids[i]] = user_ratings[i]
        combinations = list(itertools.combinations(user_ids, 2))
        for combination in combinations:
            max_possible_jaccard = float(min(user[combination[0]], user[combination[1]]))/float((user[combination[0]] + user[combination[1]] - min(user[combination[0]], user[combination[1]])))
            if max_possible_jaccard >= 0.5:
                yield [combination, [1, [user[combination[0]], user[combination[1]]]]]

    # Amount of movies rated by the pair of users.
    def reducer2(self, key, values):
        users_rating = None
        amount_movies = []
        for value in values:
            amount_movies.append(value[0])
            if users_rating is None:
                users_rating = value[1]
        yield [key, [sum(amount_movies), users_rating]]
    # **

    def reducer21(self, key, values):
        users_rating = None
        movies_in_common = []
        for value in values:
            movies_in_common.append(value[0])
            if users_rating is None:
                users_rating = value[1]
        for i in xrange(0, len(movies_in_common)):
            jaccard = float(movies_in_common[i])/float((users_rating[0])+(users_rating[1])-movies_in_common[i])
            if jaccard >= 0.5:
                yield [key, jaccard]

    def steps(self):
        return [MRStep(mapper=self.mapper_user_movies, reducer=self.reducer0),
                MRStep(mapper=self.mapper, reducer=self.reducer),
                MRStep(reducer=self.reducer2), MRStep(reducer=self.reducer21)]

if __name__ == '__main__':
    time_start = time.clock()
    MRWordFrequencyCount.run()
    time_end = time.clock()
    print "Time taken to completion of the metric: {0} in processor time".format(time_end - time_start)
