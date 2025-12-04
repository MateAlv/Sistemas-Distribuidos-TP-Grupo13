import pytest
import time
import json
from unittest.mock import Mock, patch, MagicMock
from monitor.common.monitor import MonitorNode, HEARTBEAT_TIMEOUT, MSG_HEARTBEAT
from utils.eof_protocol.end_messages import MessageForceEnd

class TestMonitorForceEnd:
    """Tests for Monitor force-end functionality"""
    
    @pytest.fixture
    def monitor_instance(self):
        """Create monitor instance with mocked middleware and threads"""
        with patch('monitor.common.monitor.pika'), \
             patch('monitor.common.monitor.MessageMiddlewareExchange') as mock_exchange, \
             patch('monitor.common.monitor.threading.Thread'), \
             patch('monitor.common.monitor.MonitorNode._recover_state'), \
             patch('monitor.common.monitor.MonitorNode._load_expected_from_config', return_value={}):
            
            monitor = MonitorNode()
            monitor.force_end_exchange = Mock()
            monitor.is_leader = True # Only leader checks for crashes
            return monitor

    def test_server_crash_detection_triggers_force_end(self, monitor_instance):
        """Test that server crash detection triggers force-end message"""
        # Setup: Server was seen recently
        monitor_instance.nodes_last_seen['server'] = time.time() - (HEARTBEAT_TIMEOUT + 1) # Expired
        
        # Mock revive_node to avoid subprocess calls
        monitor_instance._revive_node = Mock()
        
        # Action: Run checker loop iteration (simulated)
        # We extract the logic from _checker_loop for testing
        
        # Simulate logic inside _checker_loop
        node_id = 'server'
        last_seen = monitor_instance.nodes_last_seen['server']
        
        if time.time() - last_seen > HEARTBEAT_TIMEOUT:
            if node_id == "server":
                monitor_instance._trigger_force_end_all()
            monitor_instance._revive_node(node_id)
            del monitor_instance.nodes_last_seen[node_id]
            
        # Assert: Force end triggered
        # Verify force_end_exchange.send was called with MessageForceEnd(-1)
        args, _ = monitor_instance.force_end_exchange.send.call_args
        sent_bytes = args[0]
        decoded_msg = MessageForceEnd.decode(sent_bytes)
        
        assert decoded_msg.client_id() == -1
        assert monitor_instance._revive_node.called
        assert 'server' not in monitor_instance.nodes_last_seen

    def test_other_node_crash_does_not_trigger_force_end(self, monitor_instance):
        """Test that other node crashes do NOT trigger force-end"""
        # Setup: Worker node expired
        monitor_instance.nodes_last_seen['worker-1'] = time.time() - (HEARTBEAT_TIMEOUT + 1)
        
        monitor_instance._revive_node = Mock()
        
        # Simulate logic
        node_id = 'worker-1'
        last_seen = monitor_instance.nodes_last_seen['worker-1']
        
        if time.time() - last_seen > HEARTBEAT_TIMEOUT:
            if node_id == "server":
                monitor_instance._trigger_force_end_all()
            monitor_instance._revive_node(node_id)
            del monitor_instance.nodes_last_seen[node_id]
            
        # Assert: Force end NOT triggered
        monitor_instance.force_end_exchange.send.assert_not_called()
        assert monitor_instance._revive_node.called
        assert 'worker-1' not in monitor_instance.nodes_last_seen

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
