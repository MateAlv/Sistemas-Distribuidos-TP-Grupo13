import unittest
from unittest.mock import patch, MagicMock, ANY
import sys
import os
import json
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from utils.monitor.monitor import Monitor
from utils.protocol import *

class TestMonitor(unittest.TestCase):
    def setUp(self):
        # Mock environment variable for container name
        with patch.dict(os.environ, {'CONTAINER_NAME': 'test_node_1'}):
            self.monitor = Monitor()
            # Stop threads from starting automatically if we were to call start (we won't)
            self.monitor.running = False 

    def test_initialization(self):
        self.assertEqual(self.monitor.node_id, 'test_node_1')
        self.assertFalse(self.monitor.is_leader)
        self.assertIsNone(self.monitor.leader_id)

    def test_pulse(self):
        initial_pulse = self.monitor.last_pulse
        time.sleep(0.01)
        self.monitor.pulse()
        self.assertGreater(self.monitor.last_pulse, initial_pulse)

    @patch('utils.monitor.monitor.pika.BlockingConnection')
    def test_get_connection(self, mock_connection):
        self.monitor._get_connection()
        mock_connection.assert_called_once()

    @patch('utils.monitor.monitor.Monitor._get_connection')
    def test_broadcast(self, mock_get_conn):
        # Setup mock
        mock_channel = MagicMock()
        mock_get_conn.return_value.channel.return_value = mock_channel

        # Execute
        self.monitor._broadcast(MSG_ELECTION)

        # Assert
        mock_channel.exchange_declare.assert_called_with(exchange=CONTROL_EXCHANGE, exchange_type='topic', durable=True)
        
        # Verify publish call
        args, kwargs = mock_channel.basic_publish.call_args
        self.assertEqual(kwargs['exchange'], CONTROL_EXCHANGE)
        self.assertEqual(kwargs['routing_key'], "election.broadcast")
        
        body = json.loads(kwargs['body'])
        self.assertEqual(body['type'], MSG_ELECTION)
        self.assertEqual(body['id'], 'test_node_1')

    def test_handle_heartbeat_leader(self):
        # Scenario: We receive a heartbeat from a leader
        body = json.dumps({
            'type': MSG_HEARTBEAT,
            'id': 'leader_node',
            'is_leader': True,
            'timestamp': time.time()
        })
        
        self.monitor._handle_message(body, None)
        
        self.assertEqual(self.monitor.leader_id, 'leader_node')
        # last_leader_heartbeat should be updated (close to now)
        self.assertAlmostEqual(self.monitor.last_leader_heartbeat, time.time(), delta=1.0)

    def test_handle_election_lower_id(self):
        # Scenario: Receive ELECTION from lower ID -> We should start election
        with patch.object(self.monitor, '_start_election') as mock_start_election:
            body = json.dumps({
                'type': MSG_ELECTION,
                'id': 'test_node_0', # Lower than test_node_1
                'timestamp': time.time()
            })
            
            self.monitor._handle_message(body, None)
            
            mock_start_election.assert_called_once()

    def test_handle_election_higher_id(self):
        # Scenario: Receive ELECTION from higher ID -> We do nothing (wait for them to become leader)
        with patch.object(self.monitor, '_start_election') as mock_start_election:
            body = json.dumps({
                'type': MSG_ELECTION,
                'id': 'test_node_2', # Higher than test_node_1
                'timestamp': time.time()
            })
            
            self.monitor._handle_message(body, None)
            
            mock_start_election.assert_not_called()

    def test_handle_coordinator(self):
        # Scenario: Receive COORDINATOR message
        body = json.dumps({
            'type': MSG_COORDINATOR,
            'id': 'new_leader',
            'timestamp': time.time()
        })
        
        self.monitor._handle_message(body, None)
        
        self.assertEqual(self.monitor.leader_id, 'new_leader')
        self.assertFalse(self.monitor.election_in_progress)
        self.assertFalse(self.monitor.is_leader)

    @patch('utils.monitor.monitor.os._exit')
    def test_handle_death_certificate(self, mock_exit):
        # Scenario: Receive DEATH certificate for us
        body = json.dumps({
            'type': MSG_DEATH,
            'target': 'test_node_1'
        })
        
        self.monitor._handle_message(body, None)
        
        mock_exit.assert_called_once_with(1)

    @patch('utils.monitor.monitor.subprocess.run')
    def test_revive_node(self, mock_run):
        with patch.object(self.monitor, '_publish_death') as mock_publish_death:
            self.monitor._revive_node('dead_node')
            
            mock_publish_death.assert_called_once_with('dead_node')
            mock_run.assert_called_once_with(['docker', 'restart', 'dead_node'], check=False)

if __name__ == '__main__':
    unittest.main()
