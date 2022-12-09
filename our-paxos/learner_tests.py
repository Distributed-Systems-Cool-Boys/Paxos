import unittest
from paxos import learner, paxos_encode, mcast_sender
import socket

class LearnerTests(unittest.TestCase):

	# Returns True or False.
	def learn_one_message(self):
		s = mcast_sender()
		learner("", 1)
		msg = paxos_encode([0, 2, 0, 1])
		s.sendto(msg, "239.0.0.1 8000")



if __name__ == '__main__':
	unittest.main()
