#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>

#include <netinet/in.h>	
#include <arpa/inet.h>

#include <rdma/rdma_cma.h>

int run_client(in_addr_t server_addr, in_port_t server_port) {
  int ret;
  struct rdma_conn_param conn_param;

  /* basic building blocks */
  struct rdma_event_channel *cm_channel;
  struct rdma_cm_event *cm_event;
  struct sockaddr_in s_addr;
  struct rdma_cm_id *cm_conn_id;

  /* rdma objects */
  struct ibv_pd *pd;
  struct ibv_comp_channel *comp_channel;
  struct ibv_cq *cq;
  struct ibv_qp_init_attr qp_init_attr;
  struct ibv_qp *qp;

  /* work completion */
  struct ibv_cq *dst_cq;
  void *dst_cq_ctx;
  struct ibv_wc wc;

  /* client tag */
  struct app_remote_info local_triplet;
  struct ibv_mr *local_triplet_mr;
  struct ibv_sge local_triplet_sge;
  static struct ibv_send_wr local_triplet_tx_wr, *bad_tx_wr;

  /* server tag */
  struct app_remote_info remote_triplet;
  struct ibv_mr *remote_triplet_mr;
  struct ibv_sge remote_triplet_sge;
  static struct ibv_recv_wr remote_triplet_rx_wr, *bad_rx_wr;

  /* rdma write destination */
  struct app_rdma_buf *rdmaw_dst;

  /* rdma read destination */
  struct app_rdma_buf *rdmar_dst;
  static struct ibv_send_wr rdmar_wr;
  struct ibv_device_attr dev_attr;

  // CM CHANNEL & ID /////////////////////////////////////////////////////////////

  cm_channel = rdma_create_event_channel();
  if (!cm_channel) {
    error("creating cm event channel failed (%m)");
    return -1;
  }

  ret = rdma_create_id(cm_channel, &cm_conn_id, (void*) 0xDEADBEEF, RDMA_PS_TCP);
  if (ret) {
    error("creating cm id failed (%m)");
    return -1;
  }
  show_rdma_cmid(cm_conn_id);

  // ADDR & ROUTE RESOLUTION /////////////////////////////////////////////////////

  /* address */
  memset(&s_addr, 0, sizeof s_addr);
  s_addr.sin_family = AF_INET;
  s_addr.sin_addr.s_addr = server_addr;
  s_addr.sin_port = server_port;
  ret = rdma_resolve_addr(cm_conn_id, NULL, (struct sockaddr*) &s_addr, 2000);
  if (ret) {
    error("failed to resolve address (%m)");
    return -1;
  }
  debug("waiting for cm event: RDMA_CM_EVENT_ADDR_RESOLVED");
  ret = rdma_get_cm_event(cm_channel, &cm_event);
  if (ret) {
    error("failed to get cm event (%m)");
    return -1;
  }
  if (cm_event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    error("wrong event received: %s", cm_ev2str(cm_event->event));
    return -1;
  } else if (cm_event->status != 0) {
    error("event has error status: %d", cm_event->status);
    return -1;
  }
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    error("failed to acknowledge cm event (%m)");
    return -1;
  }
  show_rdma_cmid(cm_conn_id);

  /* route */
  ret = rdma_resolve_route(cm_conn_id, 2000);
  if (ret) {
    error("failed to resolve route (%m)");
    return -1;
  }
  debug("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED");
  ret = rdma_get_cm_event(cm_channel, &cm_event);
  if (ret) {
    error("failed to get cm event (%m)");
    return -1;
  }
  if (cm_event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    error("wrong event received: %s", cm_ev2str(cm_event->event));
    return -1;
  } else if (cm_event->status != 0) {
    error("event has error status: %d", cm_event->status);
    return -1;
  }
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    error("failed to acknowledge cm event (%m)");
    return -1;
  }
  show_rdma_cmid(cm_conn_id);

  // BASIC RDMA RESOURCES ////////////////////////////////////////////////////////

  pd = ibv_alloc_pd(cm_conn_id->verbs);
  if (!pd) {
    error("failed to alloc pd (%m)");
    return -1;
  }
  debug("pd allocated with handle %d \n", pd->handle);

  comp_channel = ibv_create_comp_channel(cm_conn_id->verbs);
  if (!comp_channel) {
    error("failed to create completion event channel (%m)");
    return -1;
  }
  debug("completion event channel created");
  ret = ibv_query_device(comp_channel->context, &dev_attr);
  if(ret){
    error("failed to device attributes (%m)");
    return ret;
  }
  debug(" maximum number of CQE are: %d \n", dev_attr.max_cqe);
  debug(" maximum number of CQ are: %d \n", dev_attr.max_cq);

  cq = ibv_create_cq(cm_conn_id->verbs, N_CQE, NULL, comp_channel, 0);
  /* what does the last argument do (int comp_vector)?! */
  if (!cq) {
    error("failed to create cq (%m)");
    return -1;
  }
  debug("cq created with %d elements \n", cq->cqe);
  debug("Now I am going to try resizing it to %d elements", 2*N_CQE);
  ret = ibv_resize_cq(cq, 2*N_CQE);
  if(ret){
	  printf("ERROR: failed resizing : %d \n", ret);
	  printf("Ignoring and continuing....\n");
  }else{
	  debug("Wow, was able to resize, not cq->cqe is %d\n", cq->cqe);
  }
  ret = ibv_req_notify_cq(cq, 0);
  if (ret) {
    error("failed to request notifications (%m)");
    return -1;
  }

  memset(&qp_init_attr, 0, sizeof qp_init_attr);
  qp_init_attr.cap.max_recv_sge = 4;
  qp_init_attr.cap.max_recv_wr = 10;
  qp_init_attr.cap.max_send_sge = 4;
  qp_init_attr.cap.max_send_wr = 10;
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.recv_cq = cq;
  qp_init_attr.send_cq = cq;
  ret = rdma_create_qp(cm_conn_id, pd, &qp_init_attr);
  if (ret) {
    error("failed to create qp (%m)");
    return -1;
  }
  qp = cm_conn_id->qp;
  debug("qp created with handle %d \n", qp->handle);
  show_rdma_cmid(cm_conn_id);

  // RDMA BUFFER /////////////////////////////////////////////////////////////////

  rdmaw_dst = buffer_create(pd, 12,
      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
  if (!rdmaw_dst) {
    error("failed to create rdma write destination buffer");
    return -1;
  }

  local_triplet.addr = (uint64_t) (unsigned long) rdmaw_dst->addr;
  local_triplet.length = rdmaw_dst->sge.length;
  local_triplet.rkey = rdmaw_dst->mr->rkey;
  /* mr on local triplet for remote buffer advertisement */
  local_triplet_mr = ibv_reg_mr(pd, &local_triplet, sizeof local_triplet, 0);
  if (!local_triplet_mr) {
    error("failed to create mr on local triplet (%m)");
    return -1;
  }
  /* sgl on the above mr (1 element only) */
  local_triplet_sge.addr = (uint64_t) (unsigned long) &local_triplet;
  local_triplet_sge.length = sizeof(local_triplet);
  local_triplet_sge.lkey = local_triplet_mr->lkey;
  /* tx wr on the above sgl */
  local_triplet_tx_wr.opcode = IBV_WR_SEND;
  local_triplet_tx_wr.send_flags = IBV_SEND_SIGNALED;
  local_triplet_tx_wr.sg_list = &local_triplet_sge;
  local_triplet_tx_wr.num_sge = 1;

  /* mr for receiving the remote buffer advertisement */
  remote_triplet_mr = ibv_reg_mr(pd, &remote_triplet, sizeof remote_triplet,
      IBV_ACCESS_LOCAL_WRITE);
  if (!remote_triplet_mr) {
    error("failed to create mr for server tag (%m)");
    return -1;
  }
  /* sgl on the above mr (1 element only) */
  remote_triplet_sge.addr = (uint64_t) (unsigned long) remote_triplet_mr->addr;
  remote_triplet_sge.length = remote_triplet_mr->length;
  remote_triplet_sge.lkey = remote_triplet_mr->lkey;
  /* rx wr on the above sgl */
  remote_triplet_rx_wr.sg_list = &remote_triplet_sge;
  remote_triplet_rx_wr.num_sge = 1;

  /* rdma read destination buffer */
  rdmar_dst = buffer_create(pd, 12,
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  if (!rdmar_dst) {
    error("failed to create rdma read destination buffer (%m)");
    return -1;
  }
  rdmar_wr.opcode = IBV_WR_RDMA_READ;
  rdmar_wr.send_flags = IBV_SEND_SIGNALED;
  rdmar_wr.sg_list = &rdmar_dst->sge;
  rdmar_wr.num_sge = 1;

  // CONNECT /////////////////////////////////////////////////////////////////////

  memset(&conn_param, 0, sizeof(conn_param));
  conn_param.initiator_depth = 5;
  conn_param.responder_resources = 5;
  conn_param.retry_count = 2;
  ret = rdma_connect(cm_conn_id, &conn_param);
  if (ret) {
    error("failed to connect to remote host (%m)");
    return -1;
  }
  debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED");
  ret = rdma_get_cm_event(cm_channel, &cm_event);
  if (ret) {
    error("failed to get cm event (%m)");
    return -1;
  }
  if (cm_event->event != RDMA_CM_EVENT_ESTABLISHED) {
    error("wrong event received: %s", cm_ev2str(cm_event->event));
    return -1;
  } else if (cm_event->status != 0) {
    error("event has error status: %d", cm_event->status);
    return -1;
  }
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    error("failed to acknowledge cm event (%m)");
    return -1;
  }

  // POST RECEIVE FOR REMOTE BUFFER ADVERTISEMENT ////////////////////////////////

  ret = ibv_post_recv(qp, &remote_triplet_rx_wr, &bad_rx_wr);
  if (ret) {
    error("failed to post receive wr for server tag (%m)");
    return -1;
  }

  // SEND BUFFER ADVERTISEMENT ///////////////////////////////////////////////////

  ret = ibv_post_send(qp, &local_triplet_tx_wr, &bad_tx_wr);
  if (ret) {
    error("failed to post send wr for client tag (%m)");
    return -1;
  }
  debug("waiting for send wc");
  ret = ibv_get_cq_event(comp_channel, &dst_cq, &dst_cq_ctx);
  if (ret) {
    error("failed to get next cq event (%m)");
    return -1;
  }
  ret = ibv_req_notify_cq(dst_cq, 0);
  if (ret) {
    error("failed to request notification for next cq event (%m)");
    return -1;
  }
  ret = ibv_poll_cq(cq, 1, &wc);
  if (ret < 0) {
    error("failed to poll cq for next wc (%m)");
    return -1;
  }
  if (wc.status != IBV_WC_SUCCESS) {
    error("wc has error status: %d", wc.status);
    return -1;
  }
  if (wc.opcode != IBV_WC_SEND) {
    error("unexpected wc opcode: %d", wc.opcode);
    return -1;
  }
  ibv_ack_cq_events(dst_cq, 1);
  debug("buffer advertisement sent");

  debug("waiting for remote buffer advertisement");
  ret = ibv_get_cq_event(comp_channel, &dst_cq, &dst_cq_ctx);
  if (ret) {
    error("failed to get next cq event (%m)");
    return -1;
  }
  ret = ibv_req_notify_cq(dst_cq, 0);
  if (ret) {
    error("failed to request notification for next cq event (%m)");
    return -1;
  }
  ret = ibv_poll_cq(cq, 1, &wc);
  if (ret < 0) {
    error("failed to poll cq for next wc (%m)");
    return -1;
  }
  if (wc.status != IBV_WC_SUCCESS) {
    error("wc has error status: %d", wc.status);
    return -1;
  }
  if (wc.opcode != IBV_WC_RECV) {
    error("unexpected wc opcode: %d", wc.opcode);
    return -1;
  }
  ibv_ack_cq_events(dst_cq, 1);
  debug("remote buffer advertisement received");
  rdmar_wr.wr.rdma.remote_addr = remote_triplet.addr;
  rdmar_wr.wr.rdma.rkey = remote_triplet.rkey;

  // RDMA READ ///////////////////////////////////////////////////////////////////

  ret = ibv_post_send(qp, &rdmar_wr, &bad_tx_wr);
  if (ret) {
    error("failed to post rdma read wr (%m)");
    return -1;
  }
  debug("waiting for rdma read wc");
  ret = ibv_get_cq_event(comp_channel, &dst_cq, &dst_cq_ctx);
  if (ret) {
    error("failed to get next cq event (%m)");
    return -1;
  }
  ret = ibv_req_notify_cq(dst_cq, 0);
  if (ret) {
    error("failed to request notification for next cq event (%m)");
    return -1;
  }
  ret = ibv_poll_cq(cq, 1, &wc);
  if (ret < 0) {
    error("failed to poll cq for next wc (%m)");
    return -1;
  }
  if (wc.status != IBV_WC_SUCCESS) {
    error("wc has error status: %d", wc.status);
    return -1;
  }
  if (wc.opcode != IBV_WC_RDMA_READ) {
    error("unexpected wc opcode: %d", wc.opcode);
    return -1;
  }
  ibv_ack_cq_events(dst_cq, 1);
  debug("rdma read completed");

  /* atr 12 for strlen("Hello World");
   */
  ret = memcmp(rdmar_dst->addr, rdmaw_dst->addr, 12);
  if (ret) {
    error("the buffers do NOT match");
    error("rdmaw_dst: %s", rdmaw_dst->addr);
    error("rdmar_dst: %s", rdmar_dst->addr);
  } else {
    printf("\n\nRESULT: the buffers match!\n\n");
  }

  // DISCONNECT //////////////////////////////////////////////////////////////////

  debug("disconnecting");
  ret = rdma_disconnect(cm_conn_id);
  if (ret) {
    error("disconnect failed (%m)");
    return -1;
  }
  debug("waiting for cm event: DISCONNECTED");
  ret = rdma_get_cm_event(cm_channel, &cm_event);
  if (ret) {
    error("failed to get cm event (%m)");
    return -1;
  }
  if (cm_event->event != RDMA_CM_EVENT_DISCONNECTED) {
    error("wrong event received: %s", cm_ev2str(cm_event->event));
    return -1;
  } else if (cm_event->status != 0) {
    error("event has error status: %d", cm_event->status);
    return -1;
  }
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    error("failed to acknowledge cm event (%m)");
    return -1;
  }
  debug("disconnect completed");

  // CLEANUP /////////////////////////////////////////////////////////////////////

  return 0;

}

int run_server(in_addr_t server_addr, in_port_t server_port) {
  int ret;
  struct rdma_conn_param conn_param;

  /* basic building blocks */
  struct rdma_event_channel *cm_channel;
  struct rdma_cm_event *cm_event;
  struct sockaddr_in s_addr;
  struct rdma_cm_id *cm_listen_id, *cm_conn_id;

  /* rdma objects */
  struct ibv_pd *pd;
  struct ibv_comp_channel *comp_channel;
  struct ibv_cq *cq;
  struct ibv_qp_init_attr qp_init_attr;
  struct ibv_qp *qp;

  /* work completion */
  struct ibv_cq *dst_cq;
  void *dst_cq_ctx;
  struct ibv_wc wc;

  /* local buffer description */
  struct app_remote_info local_triplet;
  struct ibv_mr *local_triplet_mr;
  struct ibv_sge local_triplet_sge;
  static struct ibv_send_wr local_triplet_tx_wr, *bad_tx_wr;

  /* remote buffer description */
  struct app_remote_info remote_triplet;
  struct ibv_mr *remote_triplet_mr;
  struct ibv_sge remote_triplet_sge;
  static struct ibv_recv_wr remote_triplet_rx_wr, *bad_rx_wr;

  /* rdma write source buffer */
  struct app_rdma_buf *rdmaw_src;

  /* rdma write destination work request */
  static struct ibv_send_wr rdmaw_wr;
  struct ibv_device_attr dev_attr;

  // CM CHANNEL & ID /////////////////////////////////////////////////////////////

  /*
   * Step 1: 
   * Open a channel used to report asynchronous communication events	
   */

  cm_channel = rdma_create_event_channel();
  if (!cm_channel) {
    error("creating cm event channel failed (%m)");
    return -1;
  } else
    debug("cm event channel created");

  /* 
   * Step 2:
   * Socket equivalent of RDMA world. Function takes, RDMA channel to which 
   * it has to create an id, rdma_cm_id ** where identifier will be stored,
   * user defined context which can be saved and retived later in scenarios
   * such as servering multiple clients, and associated RDMA port space: could
   * be either TCP type streaming (RDMA_PS_TCP) or UDP (RMDA_PS_UDP).
   * It also exports a file descriptor which can be manipulated like other 
   * FDs (select, poll, or block). 
   */

  ret = rdma_create_id(cm_channel, &cm_listen_id, NULL, RDMA_PS_TCP);
  if (ret) {
    error("creating listen cm id failed (%m)");
    return -1;
  } else
    debug("listen cm id created");

  // BINDING /////////////////////////////////////////////////////////////////////

  memset(&s_addr, 0, sizeof s_addr);
  s_addr.sin_family = AF_INET;
  s_addr.sin_addr.s_addr = server_addr;
  s_addr.sin_port = server_port;

  /* 
   * Step 3:
   * Binds the RDMA data tranfer (rdma_cm_id generated in last step) 
   * to a particular "traditonal" address as used in a socket 
   * programming.	
   *
   * -- If more than one RNICs are present in the machine then one 
   *    can bind to a particular IP address and associated RNIC. 
   *  OR
   * --  rdma_cm_id can be associated with a RDMA port number only which 
   *     will result in listening across all RNICs. 
   */

  ret = rdma_bind_addr(cm_listen_id, (struct sockaddr*) &s_addr);
  if (ret) {
    error("failed to bind server address (%m)");
    return -1;
  } else
    debug("binding done");

  /*
   * Step 4:
   * Start listening to the events which are generated on a particular 
   * channel. 
   */

  ret = rdma_listen(cm_listen_id, 1); /* backlog = 1 */
  if (ret) {
    error("failed to listen on server address (%m)");
    return -1;
  } else
    debug("Listening to the port");

  // WAIT FOR CONNECTION REQUEST /////////////////////////////////////////////////

  debug("waiting for: RDMA_CM_EVENT_CONNECT_REQUEST");

  /*
   * Step 5: 
   * This is a blocking call which tries to retrive new event from the 
   * event queue (if there is any). Associated struct details can be 
   * found in man pages ..cm_event !!
   */

  ret = rdma_get_cm_event(cm_channel, &cm_event);
  if (ret) {
    error("failed to get cm event (%m)");
    return -1;
  } else
    debug("New event found ..now matching");
  if (cm_event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
    error("wrong event received: %s", cm_ev2str(cm_event->event));
    return -1;
  } else if (cm_event->status != 0) {
    error("event has error status: %d", cm_event->status);
    return -1;
  }
  // Id associated with the event itself. Another one is id associated 
  // with the listening event. 
  /* private data are at cm_event as well... do not care here */

  cm_conn_id = cm_event->id;

  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    error("failed to acknowledge cm event (%m)");
    return -1;
  }

  // BASIC RDMA RESOURCES ////////////////////////////////////////////////////////

  /*
   * Step 6: 
   * Now with associated retrived event -- setup a new protection domain 
   * from its context. 
   *
   * Protection domain = QP, CQ, sTag etc. etc. It is checked by RNIC before
   * doing any data movement activies. 
   */

  pd = ibv_alloc_pd(cm_conn_id->verbs);
  if (!pd) {
    error("failed to alloc pd (%m)");
    return -1;
  }
  debug("pd allocated");

  /*  -- Now we will allocate component of the PD step by step. 
   *
   * Step 6.1: Create a completion channel for that particular context. It 
   * will be used to get notification on recv (and other send, read, write
   * events which are marked for generating events)
   */

  comp_channel = ibv_create_comp_channel(cm_conn_id->verbs);
  if (!comp_channel) {
    error("failed to create completion event channel (%m)");
    return -1;
  }
  debug("completion event channel created");
  ret = ibv_query_device(comp_channel->context, &dev_attr);
  if(ret){
    error("failed to device attributes (%m)");
    return ret;
  }
  debug(" maximum number of CQE are: %d \n", dev_attr.max_cqe);
  debug(" maximum number of CQ are: %d \n", dev_attr.max_cq);

  /*
   * Step 6.2: Creates explicit completion queues associated with 
   * a particular event completion channel.  
   */
  cq = ibv_create_cq(cm_conn_id->verbs, N_CQE, NULL, comp_channel, 0);

  /* what does the last argument do (int comp_vector)?! */
  debug("atr:Associated comp_vector %d", cm_conn_id->verbs->num_comp_vectors);

  if (!cq) {
    error("failed to create cq (%m)");
    return -1;
  }
  debug("cq created");

  /*
   * Step 6.3: Ask for the event for all activities in the queue. 
   *           Little bit less intutive 0 = generate event 
   *                                    1 = ?
   */

  ret = ibv_req_notify_cq(cq, 0);
  if (ret) {
    error("failed to request notifications (%m)");
    return -1;
  }

  /*
   * Step 6.4: Now the last step, set up the queue pair (send, recv) 
   * queue. All activities which is !recv goes to send queue and 
   * recv is for receiving the send data from remote hosts.
   */

  /* atr: Instead of statically assigning these numbers 
   * one should either check with application what they 
   * want or ask RDMA provider what is the maximum they 
   * can support. All have different numbers depending upon
   * hardware and firmware limitations. 
   *
   * Recommended way: To get this data and whole lot more 
   *  		ibv_query_device(...) 
   */
  memset(&qp_init_attr, 0, sizeof qp_init_attr);
  qp_init_attr.cap.max_recv_sge = 4;
  qp_init_attr.cap.max_recv_wr = 10; // So 10 entries with 4 sge (=40)
  qp_init_attr.cap.max_send_sge = 4;
  qp_init_attr.cap.max_send_wr = 10;
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.recv_cq = cq;
  qp_init_attr.send_cq = cq;
  /* 
   * Step 7:
   * cm_conn-id was extracted from the event identifier from the event
   * queue!!
   *
   * Params: The associated connection identifier, 
   *         protection domain, 
   *         and init_attr
   */

  ret = rdma_create_qp(cm_conn_id, pd, &qp_init_attr);
  if (ret) {
    error("failed to create qp (%m)");
    return -1;
  }
  qp = cm_conn_id->qp;
  debug("qp created");

  // RDMA BUFFERS ////////////////////////////////////////////////////////////////

  /* write source buffer 
   *
   * Local funtion call to setup buffers 
   */

  rdmaw_src = buffer_create(pd, 12, IBV_ACCESS_REMOTE_READ);
  if (!rdmaw_src) {
    error("failed to create rdma write source buffer");
    return -1;
  }

  /* write work request */
  rdmaw_wr.opcode = IBV_WR_RDMA_WRITE;
  rdmaw_wr.send_flags = IBV_SEND_SIGNALED;
  rdmaw_wr.sg_list = &rdmaw_src->sge;
  rdmaw_wr.num_sge = 1;

  /* write source buffer triplet for remote access */
  /*  XXX: atr - why have to cast 2 times ?*/
  local_triplet.addr = (uint64_t) (unsigned long) rdmaw_src->addr;
  local_triplet.length = rdmaw_src->sge.length;
  local_triplet.rkey = rdmaw_src->mr->rkey;
  /* memory region on write source triplet for buffer advertisement */
  local_triplet_mr = ibv_reg_mr(pd, &local_triplet, sizeof local_triplet,
      IBV_ACCESS_REMOTE_READ);
  if (!local_triplet_mr) {
    error("failed to register mr for server tag (%m)");
    return -1;
  }
  /* scatter-gather list on above memory region */
  local_triplet_sge.addr = (uint64_t) (unsigned long) local_triplet_mr->addr;
  local_triplet_sge.length = local_triplet_mr->length;
  local_triplet_sge.lkey = local_triplet_mr->lkey;
  /* send work request on above scatter-gather list */
  local_triplet_tx_wr.opcode = IBV_WR_SEND;
  local_triplet_tx_wr.send_flags = IBV_SEND_SIGNALED;
  local_triplet_tx_wr.sg_list = &local_triplet_sge;
  local_triplet_tx_wr.num_sge = 1;

  /* memory region for receiving buffer advertisement from remote host */
  remote_triplet_mr = ibv_reg_mr(pd, &remote_triplet, sizeof remote_triplet,
      IBV_ACCESS_LOCAL_WRITE);
  if (!remote_triplet_mr) {
    error("failed to create mr for client tag buffer (%m)");
    return -1;
  }
  /* sge */
  remote_triplet_sge.addr = (uint64_t) (unsigned long) &remote_triplet;
  remote_triplet_sge.length = sizeof(remote_triplet);
  remote_triplet_sge.lkey = remote_triplet_mr->lkey;
  /* wr */
  remote_triplet_rx_wr.sg_list = &remote_triplet_sge;
  remote_triplet_rx_wr.num_sge = 1;

  // POST RECEIVE FOR REMOTE BUFFER ADVERTISEMENT ////////////////////////////////

  ret = ibv_post_recv(qp, &remote_triplet_rx_wr, &bad_rx_wr);
  if (ret) {
    error("posting rx wr for remote triplet failed (%m)");
    return -1;
  }

  // ACCEPT CONNECTION ///////////////////////////////////////////////////////////

  memset(&conn_param, 0, sizeof(conn_param));
  conn_param.initiator_depth = 5;
  conn_param.responder_resources = 5;
  /* private data for connection response are at conn_param... not used */
  ret = rdma_accept(cm_conn_id, &conn_param);
  if (ret) {
    error("failed to accept connection (%m)");
    return -1;
  }
  debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED");
  ret = rdma_get_cm_event(cm_channel, &cm_event);
  if (ret) {
    error("failed to get cm event (%m)");
    return -1;
  }
  if (cm_event->event != RDMA_CM_EVENT_ESTABLISHED) {
    error("wrong event received: %s", cm_ev2str(cm_event->event));
    return -1;
  } else if (cm_event->status != 0) {
    error("event has error status: %d", cm_event->status);
    return -1;
  }
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    error("failed to acknowledge cm event (%m)");
    return -1;
  }

  // RDMA RECEIVE ////////////////////////////////////////////////////////////////

  debug("waiting for remote buffer advertisement");
  ret = ibv_get_cq_event(comp_channel, &dst_cq, &dst_cq_ctx);
  if (ret) {
    error("failed to get next cq event (%m)");
    return -1;
  }
  ret = ibv_req_notify_cq(dst_cq, 0);
  if (ret) {
    error("failed to request notification for next cq event (%m)");
    return -1;
  }
  ret = ibv_poll_cq(cq, 1, &wc);
  if (ret < 0) {
    error("failed to poll cq for next wc (%m)");
    return -1;
  }
  if (wc.status != IBV_WC_SUCCESS) {
    error("wc has error status: %d", wc.status);
    return -1;
  }
  if (wc.opcode != IBV_WC_RECV) {
    error("unexpected wc opcode: %d", wc.opcode);
    return -1;
  }
  ibv_ack_cq_events(dst_cq, 1);
  debug("remote buffer advertisement received");
  /* set destination for RDMA write (just arrived with inbound send) */
  rdmaw_wr.wr.rdma.remote_addr = remote_triplet.addr;
  rdmaw_wr.wr.rdma.rkey = remote_triplet.rkey;

  // RDMA WRITE TO THE ADVERTISED BUFFER /////////////////////////////////////////

  strcpy(rdmaw_src->addr, "Hello Write");
  ret = ibv_post_send(qp, &rdmaw_wr, &bad_tx_wr);
  if (ret) {
    error("failed to post rdma write wr (%m)");
    return -1;
  }
  debug("waiting for rdma write wc");
  ret = ibv_get_cq_event(comp_channel, &dst_cq, &dst_cq_ctx);
  if (ret) {
    error("failed to get next cq event (%m)");
    return -1;
  }
  ret = ibv_req_notify_cq(dst_cq, 0);
  if (ret) {
    error("failed to request notification for next cq event (%m)");
    return -1;
  }
  ret = ibv_poll_cq(cq, 1, &wc);
  if (ret < 0) {
    error("failed to poll cq for next wc (%m)");
    return -1;
  }
  if (wc.status != IBV_WC_SUCCESS) {
    error("wc has error status: %d", wc.status);
    return -1;
  }
  if (wc.opcode != IBV_WC_RDMA_WRITE) {
    error("unexpected wc opcode: %d", wc.opcode);
    return -1;
  }
  ibv_ack_cq_events(dst_cq, 1);
  debug("rdma write completed");

  // RDMA SEND ///////////////////////////////////////////////////////////////////

  ret = ibv_post_send(qp, &local_triplet_tx_wr, &bad_tx_wr);
  if (ret) {
    error("failed to post send wr for local buffer advertisement (%m)");
    return -1;
  }
  debug("waiting for send wc");
  ret = ibv_get_cq_event(comp_channel, &dst_cq, &dst_cq_ctx);
  if (ret) {
    error("failed to get next cq event (%m)");
    return -1;
  }
  ret = ibv_req_notify_cq(dst_cq, 0);
  if (ret) {
    error("failed to request notification for next cq event (%m)");
    return -1;
  }
  ret = ibv_poll_cq(cq, 1, &wc);
  if (ret < 0) {
    error("failed to poll cq for next wc (%m)");
    return -1;
  }
  if (wc.status != IBV_WC_SUCCESS) {
    error("wc has error status: %d", wc.status);
    return -1;
  }
  if (wc.opcode != IBV_WC_SEND) {
    error("unexpected wc opcode: %d", wc.opcode);
    return -1;
  }
  ibv_ack_cq_events(dst_cq, 1);
  debug("send completed");

  // WAIT FOR DISCONNECT /////////////////////////////////////////////////////////

  debug("waiting for cm event: RDMA_CM_EVENT_DISCONNECTED");
  ret = rdma_get_cm_event(cm_channel, &cm_event);
  if (ret) {
    error("failed to get cm event (%m)");
    return -1;
  }
  if (cm_event->event != RDMA_CM_EVENT_DISCONNECTED) {
    error("wrong event received: %s", cm_ev2str(cm_event->event));
    return -1;
  } else if (cm_event->status != 0) {
    error("event has error status: %d", cm_event->status);
    return -1;
  }
  /* private data are at cm_event as well... do not care here */
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    error("failed to acknowledge cm event (%m)");
    return -1;
  }
  debug("disconnect completed");

  // CLEANUP /////////////////////////////////////////////////////////////////////

  return 0;

}

void usage(char *str) {
  printf("Usage:\n");
  printf("Server: %s -s [-a <server_addr>] [-p <server_port>]\n", str);
  printf("Client: %s -c -a <server_addr> [-p <server_port>]\n", str);
  printf("(default port is 4711)\n");
  exit(0);
}

int main(int argc, char **argv) {
  int ret, option, is_server;
  in_port_t server_port;
  in_addr_t server_addr;

  is_server = -1;
  server_port = htons(4711);
  server_addr = 0;

  /* Parse Command Line Arguments */
  while ((option = getopt(argc, argv, "sca:p:")) != -1) {
    switch (option) {

      case 's':
        is_server = 1;
        break;
      case 'c':
        is_server = 0;
        break;
      case 'a':
        server_addr = inet_addr(optarg);
        debug("server address: %s", optarg);
        debug("server ip address %s", inet_ntoa(*(struct in_addr *)&server_addr));
        break;
      case 'p':
        server_port = strtol(optarg, NULL, 0);
        debug("server port: %s", optarg);
        break;
      default:
        usage(argv[0]);
        break;
    }
  }

  if (is_server == -1) {
    usage(argv[0]);
  } else if (is_server) {
    debug("running as server");
  } else {
    debug("running as client");
  }

  /* Run the application logic */
  if (is_server) {
    ret = run_server(server_addr, server_port);
    if (ret) {
      error("server failed");
    }
  } else {
    ret = run_client(server_addr, server_port);
    if (ret) {
      error("client failed");
    }
  }

  exit(0);

}
