#!/bin/bash
# Stop all futures trading system components

echo "🛑 Stopping Futures Trading System..."
echo ""

# Kill all Python trading processes
echo "Stopping Python components..."
pkill -f 'python.*deploy_router_node.py' && echo "  ✓ Agent router(s) stopped"
pkill -f 'python.*deploy_chat_node.py' && echo "  ✓ ChatNode(s) stopped"
pkill -f 'python.*tools_and_dashboard.py' && echo "  ✓ Tools & dashboard stopped"
pkill -f 'python.*unified_market_connector.py' && echo "  ✓ Market connector stopped"
pkill -f 'python.*response_viewer.py' && echo "  ✓ Response viewer stopped" 2>/dev/null

# Clean up PID file
rm -f logs/pids.txt

echo ""
echo "✅ All components stopped"
echo ""
echo "Note: Kafka broker is still running"
echo "To stop Kafka: cd ../calfkit-broker && make dev-down"
echo ""
