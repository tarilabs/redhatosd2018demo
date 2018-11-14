package redhatosd2018demo;

import org.kie.dmn.api.core.DMNContext;
import org.kie.dmn.api.core.DMNDecisionResult;
import org.kie.dmn.api.core.DMNResult;
import org.kie.server.api.model.ServiceResponse;
import org.kie.server.client.DMNServicesClient;
import org.kie.server.client.KieServicesClient;
import org.kie.server.client.KieServicesConfiguration;
import org.kie.server.client.KieServicesFactory;

import static redhatosd2018demo.Utils.b;
import static redhatosd2018demo.Utils.entry;
import static redhatosd2018demo.Utils.mapOf;

public class App {

    private KieServicesConfiguration conf;
    private KieServicesClient kieServicesClient;

    public void initialize() {
        conf = KieServicesFactory.newRestConfiguration(Configs.URL, Configs.USER, Configs.PASSWORD);
        conf.setMarshallingFormat(Configs.FORMAT);
        kieServicesClient = KieServicesFactory.newKieServicesClient(conf);
    }

    private void demo() {
        initialize();

        DMNServicesClient dmnClient = kieServicesClient.getServicesClient(DMNServicesClient.class);

        DMNContext dmnContext = dmnClient.newContext();
        dmnContext.set("Account holder", mapOf(entry("age", b(36)),
                                               entry("employed", true)));
        dmnContext.set("Account balance", 10000);

        String containerId = Configs.CONTAINER_ID;
        ServiceResponse<DMNResult> serverResp = dmnClient.evaluateAll(containerId, dmnContext);

        DMNResult dmnResult = serverResp.getResult();

        for (DMNDecisionResult dr : dmnResult.getDecisionResults()) {
            System.out.println("--------------------------------------------");
            System.out.println("Decision name:   " + dr.getDecisionName());
            System.out.println("Decision status: " + dr.getEvaluationStatus());
            System.out.println("Decision result: " + dr.getResult());
        }
    }

    public static void main(String[] args) {
        App a = new App();
        a.demo();
    }
}
