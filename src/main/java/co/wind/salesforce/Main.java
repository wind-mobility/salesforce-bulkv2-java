package co.wind.salesforce;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import co.wind.salesforce.response.CreateJobResponse;
import co.wind.salesforce.response.GetJobInfoResponse;
import co.wind.salesforce.type.OperationEnum;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws ConnectionException, InterruptedException {
        ConnectorConfig config = new ConnectorConfig();
        config.setUsername("etl@wind.co.partial");
        config.setPassword("KdWmECj7,6MS,9?2kSgtI5x8MLC7fek73yk0yl7G");
        config.setServiceEndpoint("https://test.salesforce.com/services/Soap/u/54.0");
        config.setAuthEndpoint("https://test.salesforce.com/services/Soap/u/54.0");
        PartnerConnection connection = Connector.newConnection(config);
        System.out.println(connection.getConfig().getSessionId());

        Bulk2Client client = new Bulk2ClientBuilder()
                .withSessionId(connection.getConfig().getSessionId(), "https://wind--partial.my.salesforce.com")
                .build();
        String csv = "UserId__c,Origin,Description,Reason,SuppliedEmail,BusinessHoursId,OwnerId,RecordTypeId,Subject\n" +
                "40dbdd07-b43c-4f2e-be9c-f872caf98213,email,Test Bulk2,Spam,hannasgjesti@hotmail.com,01m0Y000000a5FaQAI,0051v000007VEYXAA4,0120Y000000NCCzQAO,Test Bulk2";
        CreateJobResponse createJobResponse = client.createJob("Case", OperationEnum.INSERT);
        String jobId = createJobResponse.getId();
        client.uploadJobData(jobId, csv);
        client.closeJob(jobId);
        while (true) {
            TimeUnit.SECONDS.sleep(1);

            GetJobInfoResponse jobInfo = client.getJobInfo(jobId);
            System.out.println(jobInfo.getApiActiveProcessingTime());
            if (jobInfo.isFinished()) {
                break;
            }
        }
    }
}
