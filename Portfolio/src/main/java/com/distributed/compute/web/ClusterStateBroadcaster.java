package com.distributed.compute.web;

import com.distributed.compute.web.dto.ClusterStateDto;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Broadcasts cluster state to /topic/cluster-state every 500ms.
 */
@Component
public class ClusterStateBroadcaster {

    private final ClusterStateService clusterStateService;
    private final SimpMessagingTemplate messagingTemplate;

    public ClusterStateBroadcaster(ClusterStateService clusterStateService,
                                  SimpMessagingTemplate messagingTemplate) {
        this.clusterStateService = clusterStateService;
        this.messagingTemplate = messagingTemplate;
    }

    @Scheduled(fixedRate = 500)
    public void broadcastClusterState() {
        ClusterStateDto state = clusterStateService.buildClusterState();
        messagingTemplate.convertAndSend("/topic/cluster-state", state);
    }
}
