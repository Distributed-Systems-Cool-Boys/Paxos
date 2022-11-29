import unittest
from paxos import learner

class LearnerTests(unittest.TestCase):

	# Returns True or False.
	def learn_one_message(self):
		learner("", 1)

if __name__ == '__main__':
	unittest.main()
