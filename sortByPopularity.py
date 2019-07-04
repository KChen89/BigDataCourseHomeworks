from mrjob.job import MRJob 
from mrjob.step import MRStep

class sortByPopularity(MRJob):
	def steps(self):
		return [
			MRStep(mapper=self.mapper_get_ratings,
				   reducer=self.reducer_count_ratings),
			MRStep(reducer=self.reducer_sorted_output)
			]

	def mapper_get_ratings(self, key, line):
		(userID, movieID, rating, timestamp)=line.split('\t')
		yield movieID, 1

	def reducer_count_ratings(self, movieID, counts):
		yield str(sum(counts)).zfill(5), movieID

	def reducer_sorted_output(self, counts, movieID):
		for movie in movieID:
			yield movie, counts

if __name__ == '__main__':
	sortByPopularity.run()