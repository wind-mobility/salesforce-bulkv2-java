package co.wind.salesforce;

import co.wind.salesforce.request.CloseOrAbortJobRequest;
import co.wind.salesforce.request.CreateJobRequest;
import co.wind.salesforce.request.GetAllJobsRequest;
import co.wind.salesforce.response.*;
import co.wind.salesforce.type.JobStateEnum;
import co.wind.salesforce.type.OperationEnum;

import java.io.Reader;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class Bulk2Client {

    private static final String API_VERSION = "v54.0";

    private final RestRequester requester;

    private final String instanceUrl;

    public Bulk2Client(RestRequester requester, String instanceUrl) {
        this.instanceUrl = instanceUrl;
        this.requester = requester;
    }

    public CreateJobResponse createJob(String object, OperationEnum operation) {
        return createJob(object, operation, (request) -> {
        });
    }

    public CreateJobResponse createJob(String object, OperationEnum operation, Consumer<CreateJobRequest.Builder> requestBuilder) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest");

        CreateJobRequest.Builder builder = new CreateJobRequest.Builder(object, operation);
        requestBuilder.accept(builder);

        return requester.post(url, builder.build(), CreateJobResponse.class);
    }

    public CloseOrAbortJobResponse closeOrAbortJob(String jobId, JobStateEnum state) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId);

        CloseOrAbortJobRequest.Builder builder = new CloseOrAbortJobRequest.Builder(state);

        return requester.patch(url, builder.build(), CloseOrAbortJobResponse.class);
    }

    public void uploadJobData(String jobId, String csvContent) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId + "/batches");

        requester.putCsv(url, csvContent, Void.class);
    }

    public void deleteJob(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId);

        requester.delete(url, null, Void.class);
    }

    public GetAllJobsResponse getAllJobs() {
        return getAllJobs(request -> {
        });
    }

    public GetAllJobsResponse getAllJobs(Consumer<GetAllJobsRequest.Builder> requestBuilder) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest");

        GetAllJobsRequest.Builder builder = new GetAllJobsRequest.Builder();
        requestBuilder.accept(builder);

        return requester.get(url, builder.buildParameters(), GetAllJobsResponse.class);
    }

    public GetJobInfoResponse getJobInfo(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId);

        return requester.get(url, GetJobInfoResponse.class);
    }

    public void waitForJobToComplete(String jobId, Duration timeoutDuration)
            throws InterruptedException, TimeoutException {
        long millisecondsStart = System.currentTimeMillis();
        while (true) {
            GetJobInfoResponse jobInfo = getJobInfo(jobId);
            if (jobInfo.isFinished()) {
                break;
            }
            boolean timeout = System.currentTimeMillis() - millisecondsStart > timeoutDuration.toMillis();
            if (timeout) {
                abortJob(jobId);
                throw new TimeoutException("Failed to finish job after " + timeoutDuration);
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }

    public Reader getJobSuccessfulRecordResults(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId + "/successfulResults/");

        return requester.getCsv(url);
    }

    public Reader getJobFailedRecordResults(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId + "/failedResults/");

        return requester.getCsv(url);
    }

    public Reader getJobUnprocessedRecordResults(String jobId) {
        String url = buildUrl("/services/data/vXX.X/jobs/ingest/" + jobId + "/unprocessedrecords/");

        return requester.getCsv(url);
    }

    // alias

    public JobInfo closeJob(String jobId) {
        return closeOrAbortJob(jobId, JobStateEnum.UPLOAD_COMPLETE);
    }

    public JobInfo abortJob(String jobId) {
        return closeOrAbortJob(jobId, JobStateEnum.ABORTED);
    }

    private String buildUrl(String path) {
        boolean hasTrailingSlash = instanceUrl.endsWith("/");

        return instanceUrl + (hasTrailingSlash ? "/" : "") + path.replace("vXX.X", API_VERSION);
    }
}
