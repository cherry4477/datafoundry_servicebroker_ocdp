package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.OperationType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.*;
import org.springframework.cloud.servicebroker.service.ServiceInstanceService;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by baikai on 7/23/16.
 */
@Service
public class OCDPServiceInstanceService implements ServiceInstanceService {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private OCDPServiceInstanceRepository repository;

    // Operation response cache
    private Map<String, Future<CreateServiceInstanceResponse>> instanceProvisionStateMap;

    private Map<String, Future<DeleteServiceInstanceResponse>> instanceDeleteStateMap;

    public OCDPServiceInstanceService() {
        this.instanceProvisionStateMap = new HashMap<>();
        this.instanceDeleteStateMap = new HashMap<>();
    }

    @Override
    public CreateServiceInstanceResponse createServiceInstance(CreateServiceInstanceRequest request) throws OCDPServiceException {
        CreateServiceInstanceResponse response = null;
        OCDPServiceInstanceOperationService service = getAsyncOCDPServiceInstanceService();
        if(request.isAsyncAccepted()){
            Future<CreateServiceInstanceResponse> responseFuture = service.doCreateServiceInstanceAsync(request);
            try{
                this.instanceProvisionStateMap.put(request.getServiceInstanceId(), responseFuture);
                response = responseFuture.get();
            }catch (ExecutionException e){
                e.printStackTrace();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        } else {
            response = service.doCreateServiceInstance(request);
        }
        return response;
    }

    @Override
    public GetLastServiceOperationResponse getLastOperation(GetLastServiceOperationRequest request) {
        String serviceInstanceId = request.getServiceInstanceId();
        // Determine operation type: provision or delete
        OperationType operationType = getOperationType(serviceInstanceId);
        // Get Last operation response object from cache
        boolean is_operation_done = false;
        if( operationType == OperationType.PROVISION){
            Future<CreateServiceInstanceResponse> responseFuture = this.instanceProvisionStateMap.get(serviceInstanceId);
            is_operation_done = responseFuture.isDone();
        } else if( operationType == OperationType.DELETE){
            Future<DeleteServiceInstanceResponse> responseFuture = this.instanceDeleteStateMap.get(serviceInstanceId);
            is_operation_done = responseFuture.isDone();
        }
        // Return operation type
        if(is_operation_done){
            if (getOperationState(serviceInstanceId, operationType)){
                return new GetLastServiceOperationResponse().withOperationState(OperationState.SUCCEEDED);
            } else {
                return new GetLastServiceOperationResponse().withOperationState(OperationState.FAILED);
            }
        }else{
            return new GetLastServiceOperationResponse().withOperationState(OperationState.IN_PROGRESS);
        }
    }

    @Override
    public DeleteServiceInstanceResponse deleteServiceInstance(DeleteServiceInstanceRequest request)
            throws OCDPServiceException {
        DeleteServiceInstanceResponse response = null;
        OCDPServiceInstanceOperationService service = getAsyncOCDPServiceInstanceService();
        if(request.isAsyncAccepted()){
            Future<DeleteServiceInstanceResponse> responseFuture = service.doDeleteServiceInstanceAsync(request);
            try{
                this.instanceDeleteStateMap.put(request.getServiceInstanceId(), responseFuture);
                response = responseFuture.get();
            }catch (ExecutionException e){
                e.printStackTrace();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        } else {
            response = service.doDeleteServiceInstance(request);
        }
        return response;
    }

    @Override
    public UpdateServiceInstanceResponse updateServiceInstance(UpdateServiceInstanceRequest request) {
        // TODO OCDP service instance update
        return new UpdateServiceInstanceResponse();
    }

    private OCDPServiceInstanceOperationService getAsyncOCDPServiceInstanceService() {
        return (OCDPServiceInstanceOperationService)context.getBean("OCDPServiceInstanceOperationService");
    }

    private OperationType getOperationType(String serviceInstanceId){
        if (this.instanceProvisionStateMap.get(serviceInstanceId) != null){
            return OperationType.PROVISION;
        } else if (this.instanceDeleteStateMap.get(serviceInstanceId) != null){
            return OperationType.DELETE;
        } else {
            return null;
        }
    }

    private boolean getOperationState(String serviceInstanceId, OperationType operationType){
        if (operationType == OperationType.PROVISION){
            // For instance provision case, return true if instance information existed in etcd
            return (repository.findOne(serviceInstanceId) != null);
        }else if(operationType == OperationType.DELETE){
            // For instance delete case, return true if instance information not existed in etcd
            return (repository.findOne(serviceInstanceId) == null);
        } else {
            return false;
        }
    }
}
