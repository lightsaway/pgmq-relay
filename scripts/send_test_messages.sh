#!/bin/bash

# PGMQ Message Sender Script
# Connects to PGMQ running in Docker Compose and sends test messages
# Usage: ./send_test_messages.sh [options]

set -e

# Configuration
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="postgres"
POSTGRES_DB="pgmq_relay"
INTERVAL=2  # seconds between messages
DURATION=0  # 0 = infinite, otherwise seconds to run

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -i, --interval SECONDS  Interval between messages (default: 2)"
    echo "  -d, --duration SECONDS  Total duration to run (0 = infinite, default: 0)"
    echo "  --host HOST             PostgreSQL host (default: localhost)"
    echo "  --port PORT             PostgreSQL port (default: 5432)"
    echo "  --user USER             PostgreSQL user (default: postgres)"
    echo "  --password PASSWORD     PostgreSQL password (default: postgres)"
    echo "  --database DB           PostgreSQL database (default: pgmq_relay)"
    echo ""
    echo "Examples:"
    echo "  $0                      # Send messages every 2 seconds indefinitely"
    echo "  $0 -i 1 -d 60          # Send messages every 1 second for 60 seconds"
    echo "  $0 -i 0.5              # Send messages every 500ms indefinitely"
}

# Parse command line arguments
while [ $# -gt 0 ]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -i|--interval)
            INTERVAL="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        --host)
            POSTGRES_HOST="$2"
            shift 2
            ;;
        --port)
            POSTGRES_PORT="$2"
            shift 2
            ;;
        --user)
            POSTGRES_USER="$2"
            shift 2
            ;;
        --password)
            POSTGRES_PASSWORD="$2"
            shift 2
            ;;
        --database)
            POSTGRES_DB="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check if psql is available
if ! command -v psql > /dev/null 2>&1; then
    echo -e "${RED}Error: psql is not installed. Please install PostgreSQL client tools.${NC}"
    echo "On macOS: brew install postgresql"
    echo "On Ubuntu/Debian: sudo apt-get install postgresql-client"
    echo "On CentOS/RHEL: sudo yum install postgresql"
    exit 1
fi

# Connection string
export PGPASSWORD="$POSTGRES_PASSWORD"
PSQL_CMD="psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -t -c"

# Test connection
echo -e "${BLUE}Testing connection to PGMQ...${NC}"
if ! $PSQL_CMD "SELECT 1;" > /dev/null 2>&1; then
    echo -e "${RED}Error: Cannot connect to PostgreSQL at $POSTGRES_HOST:$POSTGRES_PORT${NC}"
    echo "Make sure Docker Compose is running with: docker-compose up -d"
    exit 1
fi

echo -e "${GREEN}✓ Connected to PGMQ successfully${NC}"

# Function to generate random values
generate_user_id() {
    echo "user_$(shuf -i 1000-9999 -n 1)"
}

generate_customer_id() {
    echo "cust_$(shuf -i 100-999 -n 1)"
}

generate_order_id() {
    echo "ord_$(date +%s)_$(shuf -i 100-999 -n 1)"
}

generate_transaction_id() {
    echo "txn_$(date +%s)_$(shuf -i 10000-99999 -n 1)"
}

generate_amount() {
    echo "$(shuf -i 10-1000 -n 1).$(shuf -i 10-99 -n 1)"
}

generate_ip() {
    echo "192.168.$(shuf -i 1-255 -n 1).$(shuf -i 1-255 -n 1)"
}

generate_fifo_key() {
    local queue=$1
    case $queue in
        "order_processing") echo "fifo_order_$(shuf -i 1-10 -n 1)" ;;
        "financial_transactions") echo "fifo_account_$(shuf -i 1-5 -n 1)" ;;
        "customer_changes") echo "fifo_customer_$(shuf -i 1-20 -n 1)" ;;
        *) echo "fifo_default_$(shuf -i 1-5 -n 1)" ;;
    esac
}

# Function to send a message to a specific queue
send_message() {
    queue=$1

    # Get message template based on queue
    case $queue in
        user_events) message_template='{"event": "login", "user_id": "USER_ID", "timestamp": "TIMESTAMP", "ip": "IP_ADDRESS"}' ;;
        order_processing) message_template='{"order_id": "ORDER_ID", "customer_id": "CUSTOMER_ID", "amount": AMOUNT, "status": "pending"}' ;;
        notifications) message_template='{"user_id": "USER_ID", "type": "email", "subject": "SUBJECT", "priority": "PRIORITY"}' ;;
        alerts) message_template='{"level": "LEVEL", "message": "MESSAGE", "source": "SOURCE", "timestamp": "TIMESTAMP"}' ;;
        financial_transactions) message_template='{"transaction_id": "TXN_ID", "account_id": "ACCOUNT_ID", "amount": AMOUNT, "type": "TYPE"}' ;;
        customer_changes) message_template='{"customer_id": "CUSTOMER_ID", "field": "FIELD", "old_value": "OLD_VALUE", "new_value": "NEW_VALUE"}' ;;
        *) echo -e "${RED}Error: Unknown queue '$queue'${NC}"; return 1 ;;
    esac

    # Get header template based on queue
    case $queue in
        user_events) header_template='{"source": "web", "version": "1.0"}' ;;
        order_processing) header_template='{"x-pgmq-group": "FIFO_KEY", "priority": "PRIORITY"}' ;;
        notifications) header_template='{"channel": "email", "retry_count": "0"}' ;;
        alerts) header_template='{"severity": "SEVERITY", "team": "TEAM"}' ;;
        financial_transactions) header_template='{"x-pgmq-group": "FIFO_KEY", "compliance": "required"}' ;;
        customer_changes) header_template='{"x-pgmq-group": "FIFO_KEY", "audit": "enabled"}' ;;
        *) header_template='{}' ;;
    esac

    # Generate dynamic values
    user_id=$(generate_user_id)
    customer_id=$(generate_customer_id)
    order_id=$(generate_order_id)
    transaction_id=$(generate_transaction_id)
    amount=$(generate_amount)
    timestamp=$(date -Iseconds 2>/dev/null || date +%Y-%m-%dT%H:%M:%S%z)
    ip=$(generate_ip)
    fifo_key=$(generate_fifo_key "$queue")

    # Replace placeholders in message
    message="$message_template"
    message=$(echo "$message" | sed "s/USER_ID/$user_id/g")
    message=$(echo "$message" | sed "s/CUSTOMER_ID/$customer_id/g")
    message=$(echo "$message" | sed "s/ORDER_ID/$order_id/g")
    message=$(echo "$message" | sed "s/TXN_ID/$transaction_id/g")
    message=$(echo "$message" | sed "s/ACCOUNT_ID/$customer_id/g")
    message=$(echo "$message" | sed "s/AMOUNT/$amount/g")
    message=$(echo "$message" | sed "s/TIMESTAMP/$timestamp/g")
    message=$(echo "$message" | sed "s/IP_ADDRESS/$ip/g")
    message=$(echo "$message" | sed "s/SUBJECT/Test notification $(date +%s)/g")
    message=$(echo "$message" | sed "s/PRIORITY/normal/g")
    message=$(echo "$message" | sed "s/LEVEL/info/g")
    message=$(echo "$message" | sed "s/MESSAGE/Test alert message $(date +%s)/g")
    message=$(echo "$message" | sed "s/SOURCE/test_script/g")
    message=$(echo "$message" | sed "s/TYPE/debit/g")
    message=$(echo "$message" | sed "s/FIELD/email/g")
    message=$(echo "$message" | sed "s/OLD_VALUE/old_value/g")
    message=$(echo "$message" | sed "s/NEW_VALUE/new_value/g")

    # Replace placeholders in headers
    headers="$header_template"
    headers=$(echo "$headers" | sed "s/FIFO_KEY/$fifo_key/g")
    headers=$(echo "$headers" | sed "s/PRIORITY/normal/g")
    headers=$(echo "$headers" | sed "s/SEVERITY/medium/g")
    headers=$(echo "$headers" | sed "s/TEAM/backend/g")

    # Send the message
    sql="SELECT pgmq.send(queue_name => '$queue', msg => '$message', headers => '$headers'::jsonb);"

    if result=$($PSQL_CMD "$sql" 2>&1); then
        msg_id=$(echo "$result" | tr -d ' ')
        echo -e "${GREEN}✓${NC} Sent to ${BLUE}$queue${NC} (id: $msg_id): $(echo "$message" | cut -c1-50)..."
        return 0
    else
        echo -e "${RED}✗${NC} Failed to send to ${BLUE}$queue${NC}: $result"
        return 1
    fi
}

# Function to send messages continuously
send_messages_loop() {
    start_time=$(date +%s)
    count=0
    success_count=0

    echo -e "${YELLOW}Starting message sender...${NC}"
    echo -e "Interval: ${BLUE}${INTERVAL}s${NC}"
    if [ $DURATION -gt 0 ]; then
        echo -e "Duration: ${BLUE}${DURATION}s${NC}"
    else
        echo -e "Duration: ${BLUE}infinite${NC} (press Ctrl+C to stop)"
    fi
    echo -e "Queues: ${BLUE}user_events, order_processing, notifications, alerts, financial_transactions, customer_changes${NC}"
    echo ""

    # Trap Ctrl+C for graceful shutdown
    trap 'echo -e "\n${YELLOW}Received interrupt signal. Stopping...${NC}"; break' INT

    while true; do
        # Check duration limit
        if [ $DURATION -gt 0 ]; then
            current_time=$(date +%s)
            elapsed=$((current_time - start_time))
            if [ $elapsed -ge $DURATION ]; then
                break
            fi
        fi

        # Pick a random queue
        queues_list="user_events order_processing notifications alerts financial_transactions customer_changes"
        queue_index=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); print int(rand()*6)}')
        queue=$(echo $queues_list | awk -v idx=$queue_index '{print $(idx+1)}')

        # Send message
        count=$((count + 1))
        timestamp=$(date +%Y-%m-%dT%H:%M:%S)
        echo -n "[$count] $timestamp sending to $queue... "
        if send_message "$queue"; then
            success_count=$((success_count + 1))
        fi

        # Wait for next iteration
        sleep "$INTERVAL"
    done

    echo ""
    echo -e "${YELLOW}Summary:${NC}"
    echo -e "Total messages sent: ${BLUE}$count${NC}"
    echo -e "Successful: ${GREEN}$success_count${NC}"
    echo -e "Failed: ${RED}$((count - success_count))${NC}"

    if [ $count -gt 0 ]; then
        success_rate=$(( success_count * 100 / count ))
        echo -e "Success rate: ${GREEN}${success_rate}%${NC}"
    fi
}

# Main execution
echo -e "${BLUE}PGMQ Message Sender${NC}"
echo -e "Connecting to: ${YELLOW}$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB${NC}"
echo ""

send_messages_loop

echo -e "${GREEN}Message sender completed.${NC}"
