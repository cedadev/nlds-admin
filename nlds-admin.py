from rabbit.rpc_publisher import RabbitMQRPCPublisher
from publishers.list import list_holdings
from publishers.find import find_files
from publishers.status import get_request_status

if __name__ == "__main__":
    rpc_publisher = RabbitMQRPCPublisher()
    rpc_publisher.get_connection()

    #ret = list_holdings(rpc_publisher, "", "cedaproc", groupall=True)
    #print(ret)
    #ret = find_files(rpc_publisher, "nrmassey", "cedaproc")
    #print(ret)
    ret = get_request_status(rpc_publisher, "nrmassey", "cedaproc")
    print(ret)
    rpc_publisher.close_connection()