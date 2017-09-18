import unittest
from unittest.mock import Mock

from distributed_algorithms.wave_algorithm import Wave

__author__ = 'christopher@levire.com'


class EchoWaveAlgorithm(unittest.TestCase):

    def setUp(self):
        super().setUp()

        zmocket = Mock()
        self.wave_algorithm = Wave(zmocket, False, "test_wave")

    def test_wave_init(self):

        wave_init_message = {"type": "wave_init", "sender": "sender_name"}

        send_message, message_list = self.wave_algorithm.handle_wave_message(wave_init_message)

        self.assertFalse(send_message)
        self.assertEqual(len(self.wave_algorithm.neighbors), 1)

    def test_wave_request_arrives_at_leaf(self):

        wave_request = {
            "type": "wave_request",
            "sender": "sender_name"
        }

        send_message, message_list = self.wave_algorithm.handle_wave_message(wave_request)

        self.assertTrue(send_message)
        self.assertEqual(len(message_list), 1)
        self.assertEqual(self.wave_algorithm.parent, "sender_name")

    def test_wave_request_arrives_at_node(self):

        self.wave_algorithm.neighbors.append("neighbor_1")

        wave_request = {
            "type": "wave_request",
            "sender": "sender_name"
        }

        send_message, message_list = self.wave_algorithm.handle_wave_message(wave_request)

        self.assertEqual(len(self.wave_algorithm.neighbors), 1)
        self.assertTrue(send_message)
        self.assertEqual(len(message_list), 1)
        self.assertFalse(self.wave_algorithm.wave_request["neighbor_1"])
        self.assertEqual(message_list[0]["message"], {"sender": "test_wave", "type": "wave_request"})
        self.assertEqual(message_list[0]["receiver"], "neighbor_1")

    def test_wave_leaf_response_arrives_at_node(self):
        self.wave_algorithm.neighbors.append("neighbor_1")
        self.wave_algorithm.wave_request["neighbor_1"] = False
        self.wave_algorithm.parent = "test_case"

        response = {
            "sender": "neighbor_1",
            "type": "wave_response",
            "parent": "test_wave",
            "responses": []
        }

        send_message, message_list = self.wave_algorithm.handle_wave_message(response)

        self.assertTrue(send_message)
        self.assertEqual(len(message_list), 1)
        self.assertEqual(message_list[0]["message"], {"sender": "test_wave",
                                                      "type": "wave_response",
                                                      "parent": "test_case",
                                                      "responses": [
                                                          response
                                                      ]})
        self.assertEqual(message_list[0]["receiver"], "test_case")

    def test_wave_node_response_arrives_at_node(self):
        self.wave_algorithm.neighbors.append("neighbor_1")
        self.wave_algorithm.wave_request["neighbor_1"] = False
        self.wave_algorithm.parent = "test_case"

        leaf_response = {
            "sender": "sub_neighbor_1",
            "type": "wave_response",
            "parent": "test_wave",
            "responses": []
        }

        response = {
            "sender": "neighbor_1",
            "type": "wave_response",
            "parent": "test_wave",
            "responses": [leaf_response]
        }

        send_message, message_list = self.wave_algorithm.handle_wave_message(response)

        self.assertTrue(send_message)
        self.assertEqual(len(message_list), 1)
        self.assertEqual(message_list[0]["message"], {"sender": "test_wave",
                                                      "type": "wave_response",
                                                      "parent": "test_case",
                                                      "responses": [
                                                          response
                                                      ]})
        self.assertEqual(message_list[0]["receiver"], "test_case")

    def test_init_wave(self):
        self.wave_algorithm.init_wave()

        self.assertEqual(self.wave_algorithm.wave_responses, [])
        self.assertEqual(self.wave_algorithm.wave_request, {})