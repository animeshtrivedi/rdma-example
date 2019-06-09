/*
 * Implementation of the common RDMA functions. 
 *
 * Authors: Animesh Trivedi
 *          atrivedi@apache.org 
 */

#include "rdma_common.h"

void show_rdma_cmid(struct rdma_cm_id *id)
{
	if(!id){
		rdma_error("Passed ptr is NULL\n");
		return;
	}
	printf("RDMA cm id at %p \n", id);
	if(id->verbs && id->verbs->device)
		printf("dev_ctx: %p (device name: %s) \n", id->verbs, 
				id->verbs->device->name);
	if(id->channel)
		printf("cm event channel %p\n", id->channel);
	printf("QP: %p, port_space %x, port_num %u \n", id->qp, 
			id->ps,
			id->port_num);
}

void show_rdma_buffer_attr(struct rdma_buffer_attr *attr){
	if(!attr){
		rdma_error("Passed attr is NULL\n");
		return;
	}
	printf("---------------------------------------------------------\n");
	printf("buffer attr, addr: %p , len: %u , stag : 0x%x \n", 
			(void*) attr->address, 
			(unsigned int) attr->length,
			attr->stag.local_stag);
	printf("---------------------------------------------------------\n");
}

struct ibv_mr* rdma_buffer_alloc(struct ibv_pd *pd, uint32_t size,
    enum ibv_access_flags permission) 
{
	struct ibv_mr *mr = NULL;
	if (!pd) {
		rdma_error("Protection domain is NULL \n");
		return NULL;
	}
	void *buf = calloc(1, size);
	if (!buf) {
		rdma_error("failed to allocate buffer, -ENOMEM\n");
		return NULL;
	}
	debug("Buffer allocated: %p , len: %u \n", buf, size);
	mr = rdma_buffer_register(pd, buf, size, permission);
	if(!mr){
		free(buf);
	}
	return mr;
}

struct ibv_mr *rdma_buffer_register(struct ibv_pd *pd, 
		void *addr, uint32_t length, 
		enum ibv_access_flags permission)
{
	struct ibv_mr *mr = NULL;
	if (!pd) {
		rdma_error("Protection domain is NULL, ignoring \n");
		return NULL;
	}
	mr = ibv_reg_mr(pd, addr, length, permission);
	if (!mr) {
		rdma_error("Failed to create mr on buffer, errno: %d \n", -errno);
		return NULL;
	}
	debug("Registered: %p , len: %u , stag: 0x%x \n", 
			mr->addr, 
			(unsigned int) mr->length, 
			mr->lkey);
	return mr;
}

void rdma_buffer_free(struct ibv_mr *mr) 
{
	if (!mr) {
		rdma_error("Passed memory region is NULL, ignoring\n");
		return ;
	}
	void *to_free = mr->addr;
	rdma_buffer_deregister(mr);
	debug("Buffer %p free'ed\n", to_free);
	free(to_free);
}

void rdma_buffer_deregister(struct ibv_mr *mr) 
{
	if (!mr) { 
		rdma_error("Passed memory region is NULL, ignoring\n");
		return;
	}
	debug("Deregistered: %p , len: %u , stag : 0x%x \n", 
			mr->addr, 
			(unsigned int) mr->length, 
			mr->lkey);
	ibv_dereg_mr(mr);
}

int process_rdma_cm_event(struct rdma_event_channel *echannel, 
		enum rdma_cm_event_type expected_event,
		struct rdma_cm_event **cm_event)
{
	int ret = 1;
	ret = rdma_get_cm_event(echannel, cm_event);
	if (ret) {
		rdma_error("Failed to retrieve a cm event, errno: %d \n",
				-errno);
		return -errno;
	}
	/* lets see, if it was a good event */
	if(0 != (*cm_event)->status){
		rdma_error("CM event has non zero status: %d\n", (*cm_event)->status);
		ret = -((*cm_event)->status);
		/* important, we acknowledge the event */
		rdma_ack_cm_event(*cm_event);
		return ret;
	}
	/* if it was a good event, was it of the expected type */
	if ((*cm_event)->event != expected_event) {
		rdma_error("Unexpected event received: %s [ expecting: %s ]", 
				rdma_event_str((*cm_event)->event),
				rdma_event_str(expected_event));
		/* important, we acknowledge the event */
		rdma_ack_cm_event(*cm_event);
		return -1; // unexpected event :(
	}
	debug("A new %s type event is received \n", rdma_event_str((*cm_event)->event));
	/* The caller must acknowledge the event */
	return ret;
}


int process_work_completion_events (struct ibv_comp_channel *comp_channel, 
		struct ibv_wc *wc, int max_wc)
{
	struct ibv_cq *cq_ptr = NULL;
	void *context = NULL;
	int ret = -1, i, total_wc = 0;
       /* We wait for the notification on the CQ channel */
	ret = ibv_get_cq_event(comp_channel, /* IO channel where we are expecting the notification */ 
		       &cq_ptr, /* which CQ has an activity. This should be the same as CQ we created before */ 
		       &context); /* Associated CQ user context, which we did set */
       if (ret) {
	       rdma_error("Failed to get next CQ event due to %d \n", -errno);
	       return -errno;
       }
       /* Request for more notifications. */
       ret = ibv_req_notify_cq(cq_ptr, 0);
       if (ret){
	       rdma_error("Failed to request further notifications %d \n", -errno);
	       return -errno;
       }
       /* We got notification. We reap the work completion (WC) element. It is 
	* unlikely but a good practice it write the CQ polling code that 
       * can handle zero WCs. ibv_poll_cq can return zero. Same logic as 
       * MUTEX conditional variables in pthread programming.
	*/
       total_wc = 0;
       do {
	       ret = ibv_poll_cq(cq_ptr /* the CQ, we got notification for */, 
		       max_wc - total_wc /* number of remaining WC elements*/,
		       wc + total_wc/* where to store */);
	       if (ret < 0) {
		       rdma_error("Failed to poll cq for wc due to %d \n", ret);
		       /* ret is errno here */
		       return ret;
	       }
	       total_wc += ret;
       } while (total_wc < max_wc); 
       debug("%d WC are completed \n", total_wc);
       /* Now we check validity and status of I/O work completions */
       for( i = 0 ; i < total_wc ; i++) {
	       if (wc[i].status != IBV_WC_SUCCESS) {
		       rdma_error("Work completion (WC) has error status: %s at index %d", 
				       ibv_wc_status_str(wc[i].status), i);
		       /* return negative value */
		       return -(wc[i].status);
	       }
       }
       /* Similar to connection management events, we need to acknowledge CQ events */
       ibv_ack_cq_events(cq_ptr, 
		       1 /* we received one event notification. This is not 
		       number of WC elements */);
       return total_wc; 
}


/* Code acknowledgment: rping.c from librdmacm/examples */
int get_addr(char *dst, struct sockaddr *addr)
{
	struct addrinfo *res;
	int ret = -1;
	ret = getaddrinfo(dst, NULL, NULL, &res);
	if (ret) {
		rdma_error("getaddrinfo failed - invalid hostname or IP address\n");
		return ret;
	}
	memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
	freeaddrinfo(res);
	return ret;
}

