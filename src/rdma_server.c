/*
 * This is a RDMA server side code. 
 *
 * Author: Animesh Trivedi 
 *         atrivedi@apache.org 
 *
 * TODO: Cleanup previously allocated resources in case of an error condition
 */

#include "rdma_common.h"

/* These are the RDMA resources needed to setup an RDMA connection */
/* Event channel, where connection management (cm) related events are relayed */
static struct rdma_event_channel *cm_event_channel = NULL;
static struct rdma_cm_id *cm_server_id = NULL, *cm_client_id = NULL;
static struct ibv_pd *pd = NULL;
static struct ibv_comp_channel *io_completion_channel = NULL;
static struct ibv_cq *cq = NULL;
static struct ibv_qp_init_attr qp_init_attr;
static struct ibv_qp *client_qp = NULL;
/* RDMA memory resources */
static struct ibv_mr *client_metadata_mr = NULL, *server_buffer_mr = NULL, *server_metadata_mr = NULL;
static struct rdma_buffer_attr client_metadata_attr, server_metadata_attr;
static struct ibv_recv_wr client_recv_wr, *bad_client_recv_wr = NULL;
static struct ibv_send_wr server_send_wr, *bad_server_send_wr = NULL;
static struct ibv_sge client_recv_sge, server_send_sge;

/* When we call this function cm_client_id must be set to a valid identifier.
 * This is where, we prepare client connection before we accept it. This 
 * mainly involve pre-posting a receive buffer to receive client side 
 * RDMA credentials
 */
static int setup_client_resources()
{
	int ret = -1;
	if(!cm_client_id){
		rdma_error("Client id is still NULL \n");
		return -EINVAL;
	}
	/* We have a valid connection identifier, lets start to allocate 
	 * resources. We need: 
	 * 1. Protection Domains (PD)
	 * 2. Memory Buffers 
	 * 3. Completion Queues (CQ)
	 * 4. Queue Pair (QP)
	 * Protection Domain (PD) is similar to a "process abstraction" 
	 * in the operating system. All resources are tied to a particular PD. 
	 * And accessing recourses across PD will result in a protection fault.
	 */
	pd = ibv_alloc_pd(cm_client_id->verbs 
			/* verbs defines a verb's provider, 
			 * i.e an RDMA device where the incoming 
			 * client connection came */);
	if (!pd) {
		rdma_error("Failed to allocate a protection domain errno: %d\n",
				-errno);
		return -errno;
	}
	debug("A new protection domain is allocated at %p \n", pd);
	/* Now we need a completion channel, were the I/O completion 
	 * notifications are sent. Remember, this is different from connection 
	 * management (CM) event notifications. 
	 * A completion channel is also tied to an RDMA device, hence we will 
	 * use cm_client_id->verbs. 
	 */
	io_completion_channel = ibv_create_comp_channel(cm_client_id->verbs);
	if (!io_completion_channel) {
		rdma_error("Failed to create an I/O completion event channel, %d\n",
				-errno);
		return -errno;
	}
	debug("An I/O completion event channel is created at %p \n", 
			io_completion_channel);
	/* Now we create a completion queue (CQ) where actual I/O 
	 * completion metadata is placed. The metadata is packed into a structure 
	 * called struct ibv_wc (wc = work completion). ibv_wc has detailed 
	 * information about the work completion. An I/O request in RDMA world 
	 * is called "work" ;) 
	 */
	cq = ibv_create_cq(cm_client_id->verbs /* which device*/, 
			CQ_CAPACITY /* maximum capacity*/, 
			NULL /* user context, not used here */,
			io_completion_channel /* which IO completion channel */, 
			0 /* signaling vector, not used here*/);
	if (!cq) {
		rdma_error("Failed to create a completion queue (cq), errno: %d\n",
				-errno);
		return -errno;
	}
	debug("Completion queue (CQ) is created at %p with %d elements \n", 
			cq, cq->cqe);
	/* Ask for the event for all activities in the completion queue*/
	ret = ibv_req_notify_cq(cq /* on which CQ */, 
			0 /* 0 = all event type, no filter*/);
	if (ret) {
		rdma_error("Failed to request notifications on CQ errno: %d \n",
				-errno);
		return -errno;
	}
	/* Now the last step, set up the queue pair (send, recv) queues and their capacity.
	 * The capacity here is define statically but this can be probed from the 
	 * device. We just use a small number as defined in rdma_common.h */
       bzero(&qp_init_attr, sizeof qp_init_attr);
       qp_init_attr.cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
       qp_init_attr.cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
       qp_init_attr.cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
       qp_init_attr.cap.max_send_wr = MAX_WR; /* Maximum send posting capacity */
       qp_init_attr.qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
       /* We use same completion queue, but one can use different queues */
       qp_init_attr.recv_cq = cq; /* Where should I notify for receive completion operations */
       qp_init_attr.send_cq = cq; /* Where should I notify for send completion operations */
       /*Lets create a QP */
       ret = rdma_create_qp(cm_client_id /* which connection id */,
		       pd /* which protection domain*/,
		       &qp_init_attr /* Initial attributes */);
       if (ret) {
	       rdma_error("Failed to create QP due to errno: %d\n", -errno);
	       return -errno;
       }
       /* Save the reference for handy typing but is not required */
       client_qp = cm_client_id->qp;
       debug("Client QP created at %p\n", client_qp);
       return ret;
}

/* Starts an RDMA server by allocating basic connection resources */
static int start_rdma_server(struct sockaddr_in *server_addr) 
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
	/*  Open a channel used to report asynchronous communication event */
	cm_event_channel = rdma_create_event_channel();
	if (!cm_event_channel) {
		rdma_error("Creating cm event channel failed with errno : (%d)", -errno);
		return -errno;
	}
	debug("RDMA CM event channel is created successfully at %p \n", 
			cm_event_channel);
	/* rdma_cm_id is the connection identifier (like socket) which is used 
	 * to define an RDMA connection. 
	 */
	ret = rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP);
	if (ret) {
		rdma_error("Creating server cm id failed with errno: %d ", -errno);
		return -errno;
	}
	debug("A RDMA connection id for the server is created \n");
	/* Explicit binding of rdma cm id to the socket credentials */
	ret = rdma_bind_addr(cm_server_id, (struct sockaddr*) server_addr);
	if (ret) {
		rdma_error("Failed to bind server address, errno: %d \n", -errno);
		return -errno;
	}
	debug("Server RDMA CM id is successfully binded \n");
	/* Now we start to listen on the passed IP and port. However unlike
	 * normal TCP listen, this is a non-blocking call. When a new client is 
	 * connected, a new connection management (CM) event is generated on the 
	 * RDMA CM event channel from where the listening id was created. Here we
	 * have only one channel, so it is easy. */
	ret = rdma_listen(cm_server_id, 8); /* backlog = 8 clients, same as TCP, see man listen*/
	if (ret) {
		rdma_error("rdma_listen failed to listen on server address, errno: %d ",
				-errno);
		return -errno;
	}
	printf("Server is listening successfully at: %s , port: %d \n",
			inet_ntoa(server_addr->sin_addr),
			ntohs(server_addr->sin_port));
	/* now, we expect a client to connect and generate a RDMA_CM_EVNET_CONNECT_REQUEST 
	 * We wait (block) on the connection management event channel for 
	 * the connect event. 
	 */
	ret = process_rdma_cm_event(cm_event_channel, 
			RDMA_CM_EVENT_CONNECT_REQUEST,
			&cm_event);
	if (ret) {
		rdma_error("Failed to get cm event, ret = %d \n" , ret);
		return ret;
	}
	/* Much like TCP connection, listening returns a new connection identifier 
	 * for newly connected client. In the case of RDMA, this is stored in id 
	 * field. For more details: man rdma_get_cm_event 
	 */
	cm_client_id = cm_event->id;
	/* now we acknowledge the event. Acknowledging the event free the resources 
	 * associated with the event structure. Hence any reference to the event 
	 * must be made before acknowledgment. Like, we have already saved the 
	 * client id from "id" field before acknowledging the event. 
	 */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the cm event errno: %d \n", -errno);
		return -errno;
	}
	debug("A new RDMA client connection id is stored at %p\n", cm_client_id);
	return ret;
}

/* Pre-posts a receive buffer and accepts an RDMA client connection */
static int accept_client_connection()
{
	struct rdma_conn_param conn_param;
	struct rdma_cm_event *cm_event = NULL;
	struct sockaddr_in remote_sockaddr; 
	int ret = -1;
	if(!cm_client_id || !client_qp) {
		rdma_error("Client resources are not properly setup\n");
		return -EINVAL;
	}
	/* we prepare the receive buffer in which we will receive the client metadata*/
        client_metadata_mr = rdma_buffer_register(pd /* which protection domain */, 
			&client_metadata_attr /* what memory */,
			sizeof(client_metadata_attr) /* what length */, 
		       (IBV_ACCESS_LOCAL_WRITE) /* access permissions */);
	if(!client_metadata_mr){
		rdma_error("Failed to register client attr buffer\n");
		//we assume ENOMEM
		return -ENOMEM;
	}
	/* We pre-post this receive buffer on the QP. SGE credentials is where we 
	 * receive the metadata from the client */
	client_recv_sge.addr = (uint64_t) client_metadata_mr->addr; // same as &client_buffer_attr
	client_recv_sge.length = client_metadata_mr->length;
	client_recv_sge.lkey = client_metadata_mr->lkey;
	/* Now we link this SGE to the work request (WR) */
	bzero(&client_recv_wr, sizeof(client_recv_wr));
	client_recv_wr.sg_list = &client_recv_sge;
	client_recv_wr.num_sge = 1; // only one SGE
	ret = ibv_post_recv(client_qp /* which QP */,
		      &client_recv_wr /* receive work request*/,
		      &bad_client_recv_wr /* error WRs */);
	if (ret) {
		rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
		return ret;
	}
	debug("Receive buffer pre-posting is successful \n");
	/* Now we accept the connection. Recall we have not accepted the connection 
	 * yet because we have to do lots of resource pre-allocation */
       memset(&conn_param, 0, sizeof(conn_param));
       /* this tell how many outstanding requests can we handle */
       conn_param.initiator_depth = 3; /* For this exercise, we put a small number here */
       /* This tell how many outstanding requests we expect other side to handle */
       conn_param.responder_resources = 3; /* For this exercise, we put a small number */
       ret = rdma_accept(cm_client_id, &conn_param);
       if (ret) {
	       rdma_error("Failed to accept the connection, errno: %d \n", -errno);
	       return -errno;
       }
       /* We expect an RDMA_CM_EVNET_ESTABLISHED to indicate that the RDMA  
	* connection has been established and everything is fine on both, server 
	* as well as the client sides.
	*/
        debug("Going to wait for : RDMA_CM_EVENT_ESTABLISHED event \n");
       ret = process_rdma_cm_event(cm_event_channel, 
		       RDMA_CM_EVENT_ESTABLISHED,
		       &cm_event);
        if (ret) {
		rdma_error("Failed to get the cm event, errnp: %d \n", -errno);
		return -errno;
	}
	/* We acknowledge the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		return -errno;
	}
	/* Just FYI: How to extract connection information */
	memcpy(&remote_sockaddr /* where to save */, 
			rdma_get_peer_addr(cm_client_id) /* gives you remote sockaddr */, 
			sizeof(struct sockaddr_in) /* max size */);
	printf("A new connection is accepted from %s \n", 
			inet_ntoa(remote_sockaddr.sin_addr));
	return ret;
}

/* This function sends server side buffer metadata to the connected client */
static int send_server_metadata_to_client() 
{
	struct ibv_wc wc;
	int ret = -1;
	/* Now, we first wait for the client to start the communication by 
	 * sending the server its metadata info. The server does not use it 
	 * in our example. We will receive a work completion notification for 
	 * our pre-posted receive request.
	 */
	ret = process_work_completion_events(io_completion_channel, &wc, 1);
	if (ret != 1) {
		rdma_error("Failed to receive , ret = %d \n", ret);
		return ret;
	}
	/* if all good, then we should have client's buffer information, lets see */
	printf("Client side buffer information is received...\n");
	show_rdma_buffer_attr(&client_metadata_attr);
	printf("The client has requested buffer length of : %u bytes \n", 
			client_metadata_attr.length);
	/* We need to setup requested memory buffer. This is where the client will 
	* do RDMA READs and WRITEs. */
       server_buffer_mr = rdma_buffer_alloc(pd /* which protection domain */, 
		       client_metadata_attr.length /* what size to allocate */, 
		       (IBV_ACCESS_LOCAL_WRITE|
		       IBV_ACCESS_REMOTE_READ|
		       IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
       if(!server_buffer_mr){
	       rdma_error("Server failed to create a buffer \n");
	       /* we assume that it is due to out of memory error */
	       return -ENOMEM;
       }
       /* This buffer is used to transmit information about the above 
	* buffer to the client. So this contains the metadata about the server 
	* buffer. Hence this is called metadata buffer. Since this is already 
	* on allocated, we just register it. 
        * We need to prepare a send I/O operation that will tell the 
	* client the address of the server buffer. 
	*/
       server_metadata_attr.address = (uint64_t) server_buffer_mr->addr;
       server_metadata_attr.length = (uint32_t) server_buffer_mr->length;
       server_metadata_attr.stag.local_stag = (uint32_t) server_buffer_mr->lkey;
       server_metadata_mr = rdma_buffer_register(pd /* which protection domain*/, 
		       &server_metadata_attr /* which memory to register */, 
		       sizeof(server_metadata_attr) /* what is the size of memory */,
		       IBV_ACCESS_LOCAL_WRITE /* what access permission */);
       if(!server_metadata_mr){
	       rdma_error("Server failed to create to hold server metadata \n");
	       /* we assume that this is due to out of memory error */
	       return -ENOMEM;
       }
       /* We need to transmit this buffer. So we create a send request. 
	* A send request consists of multiple SGE elements. In our case, we only
	* have one 
	*/
       server_send_sge.addr = (uint64_t) &server_metadata_attr;
       server_send_sge.length = sizeof(server_metadata_attr);
       server_send_sge.lkey = server_metadata_mr->lkey;
       /* now we link this sge to the send request */
       bzero(&server_send_wr, sizeof(server_send_wr));
       server_send_wr.sg_list = &server_send_sge;
       server_send_wr.num_sge = 1; // only 1 SGE element in the array 
       server_send_wr.opcode = IBV_WR_SEND; // This is a send request 
       server_send_wr.send_flags = IBV_SEND_SIGNALED; // We want to get notification 
       /* This is a fast data path operation. Posting an I/O request */
       ret = ibv_post_send(client_qp /* which QP */, 
		       &server_send_wr /* Send request that we prepared before */, 
		       &bad_server_send_wr /* In case of error, this will contain failed requests */);
       if (ret) {
	       rdma_error("Posting of server metdata failed, errno: %d \n",
			       -errno);
	       return -errno;
       }
       /* We check for completion notification */
       ret = process_work_completion_events(io_completion_channel, &wc, 1);
       if (ret != 1) {
	       rdma_error("Failed to send server metadata, ret = %d \n", ret);
	       return ret;
       }
       debug("Local buffer metadata has been sent to the client \n");
       return 0;
}

/* This is server side logic. Server passively waits for the client to call 
 * rdma_disconnect() and then it will clean up its resources */
static int disconnect_and_cleanup()
{
	struct rdma_cm_event *cm_event = NULL;
	int ret = -1;
       /* Now we wait for the client to send us disconnect event */
       debug("Waiting for cm event: RDMA_CM_EVENT_DISCONNECTED\n");
       ret = process_rdma_cm_event(cm_event_channel, 
		       RDMA_CM_EVENT_DISCONNECTED, 
		       &cm_event);
       if (ret) {
	       rdma_error("Failed to get disconnect event, ret = %d \n", ret);
	       return ret;
       }
	/* We acknowledge the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
		rdma_error("Failed to acknowledge the cm event %d\n", -errno);
		return -errno;
	}
	printf("A disconnect event is received from the client...\n");
	/* We free all the resources */
	/* Destroy QP */
	rdma_destroy_qp(cm_client_id);
	/* Destroy client cm id */
	ret = rdma_destroy_id(cm_client_id);
	if (ret) {
		rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy CQ */
	ret = ibv_destroy_cq(cq);
	if (ret) {
		rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy completion channel */
	ret = ibv_destroy_comp_channel(io_completion_channel);
	if (ret) {
		rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy memory buffers */
	rdma_buffer_free(server_buffer_mr);
	rdma_buffer_deregister(server_metadata_mr);	
	rdma_buffer_deregister(client_metadata_mr);	
	/* Destroy protection domain */
	ret = ibv_dealloc_pd(pd);
	if (ret) {
		rdma_error("Failed to destroy client protection domain cleanly, %d \n", -errno);
		// we continue anyways;
	}
	/* Destroy rdma server id */
	ret = rdma_destroy_id(cm_server_id);
	if (ret) {
		rdma_error("Failed to destroy server id cleanly, %d \n", -errno);
		// we continue anyways;
	}
	rdma_destroy_event_channel(cm_event_channel);
	printf("Server shut-down is complete \n");
	return 0;
}


void usage() 
{
	printf("Usage:\n");
	printf("rdma_server: [-a <server_addr>] [-p <server_port>]\n");
	printf("(default port is %d)\n", DEFAULT_RDMA_PORT);
	exit(1);
}

int main(int argc, char **argv) 
{
	int ret, option;
	struct sockaddr_in server_sockaddr;
	bzero(&server_sockaddr, sizeof server_sockaddr);
	server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
	server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
	/* Parse Command Line Arguments, not the most reliable code */
	while ((option = getopt(argc, argv, "a:p:")) != -1) {
		switch (option) {
			case 'a':
				/* Remember, this will overwrite the port info */
				ret = get_addr(optarg, (struct sockaddr*) &server_sockaddr);
				if (ret) {
					rdma_error("Invalid IP \n");
					 return ret;
				}
				break;
			case 'p':
				/* passed port to listen on */
				server_sockaddr.sin_port = htons(strtol(optarg, NULL, 0)); 
				break;
			default:
				usage();
				break;
		}
	}
	if(!server_sockaddr.sin_port) {
		/* If still zero, that mean no port info provided */
		server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT); /* use default port */
	 }
	ret = start_rdma_server(&server_sockaddr);
	if (ret) {
		rdma_error("RDMA server failed to start cleanly, ret = %d \n", ret);
		return ret;
	}
	ret = setup_client_resources();
	if (ret) { 
		rdma_error("Failed to setup client resources, ret = %d \n", ret);
		return ret;
	}
	ret = accept_client_connection();
	if (ret) {
		rdma_error("Failed to handle client cleanly, ret = %d \n", ret);
		return ret;
	}
	ret = send_server_metadata_to_client();
	if (ret) {
		rdma_error("Failed to send server metadata to the client, ret = %d \n", ret);
		return ret;
	}
	ret = disconnect_and_cleanup();
	if (ret) { 
		rdma_error("Failed to clean up resources properly, ret = %d \n", ret);
		return ret;
	}
	return 0;
}
