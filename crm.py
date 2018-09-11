# A CRM Subsystem for Global Client Solution Analytics Dashboard

from __future__ import division
import datetime,cPickle,os

global ENVIRONMENT, ENV_FILE

Dir = os.getcwd()
ENV_FILE = Dir+"/ENV_STORE"

def read(file):
    process = open(file,"rb+")
    data = cPickle.loads(process.read())
    process.close()
    return data

def write(file, data):
    process = open(file,"wb+")
    process.write(cPickle.dumps(data))
    process.close()
    return None

try:
    ENVIRONMENT = read(ENV_FILE)
except:
    ENVIRONMENT = {
        "resources":[],
        "new_leads":[],
        "benchmarks":{
            "max_close_ratio":[0,None],
            "gross_revenue":[0,None],
            "revenue_per_head":[0,None],
            "total_cost":[0,None],
            "gross_margin":[0,None],
            "customer_retention_rate":[0,None],
            "customer_churn_rate":[0,None],
            "projected_growth_rate":[0,None],
            "annual_recurring_revenue":[0,None],
            "monthly_recurring_revenue":[0,None],
            "annual_contract_value":[0,None]
        }
    };
    write(ENV_FILE,ENVIRONMENT)

def timestamp():
    return datetime.datetime.today()

class Resource:
    def __init__(self):
        self.resource_id = resource_id;
        self.activation_date = activation_date;
        self.region = region;
        self.country = country;
        self.zone = zone;
        self.role = role;
        self.supervisor_id = supervisor_id;
        self.company_phone = company_phone;
        self.company_email = company_email;
        self.biodata = {
            "first_name":first_name,
            "last_name":last_name,
            "date_of_birth":date_of_birth,
            "nationality":nationality,
            "sex":sex
            };
        self.activity = {
            "historical":{
                "closed_deals":0,
                "lost_deals":0,
                "closed_dollars":0,
                "lost_dollars":0,
                "months_active":0,
                "quotas":[],
                "periods_ended":[]
            },
            "current":{
                "period_ends":None,
                "quota":0,
                "accounts":{}
            }
        };
        self.has_met_quota = False;
        self.close_ratio = 0;
        self.deals_in_pipeline = 0;
        self.total_size_of_deals_in_pipeline = 0;
        self.average_size_of_deal_in_pipeline = 0;
        self.sales_velocity = 0;
        self.pipeline_deals_detail = None;
        self.current_deals = 0;
        self.activity_breakdown = {
            "prospecting_event":0,
            "appointment_event":0,
            "needs_analysis_event":0,
            "qualified_to_buy_event":0,
            "presentation_scheduled_event":0,
            "decision_maker_buy_in_event":0,
            "contract_sent_event":0,
            "closed_lost_event":0,
            "closed_won_event":0
        };
        self.new_accounts = 0;
        self.recurring_monthly_revenue = 0;

def personnel_search(description):
    resources = read(ENV_FILE)["resources"]
    for KEY in description:
        if KEY == "resource_id" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.resource_id == description["resource_id"]]
        if KEY == "supervisor_id" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.supervisor_id == description["supervisor_id"]]
        if KEY == "region" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.region == description["region"]]
        if KEY == "country" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.country == description["country"]]
        if KEY == "zone" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.zone == description["zone"]]
        if KEY == "role" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.role == description["role"]]
        if KEY == "first_name" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.first_name == description["first_name"]]
        if KEY == "last_name" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.last_name == description["last_name"]]
        if KEY == "date_of_birth" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.date_of_birth == description["date_of_birth"]]
        if KEY == "nationality" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.nationality == description["nationality"]]
        if KEY == "sex" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.sex == description["sex"]]
        if KEY == "company_phone" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.company_phone == description["company_phone"]]
        if KEY == "company_email" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.company_email == description["company_email"]]
        if KEY == "has_met_quota" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.has_met_quota == description["has_met_quota"]]
        if KEY == "quota_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.activity["current"]["quota"] >= description["quota_range"][0] and resource.activity["current"]["quota"] <= description["quota_range"][1]]
        if KEY == "months_active_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.activity["historical"]["months_active"] >= description["months_active_range"][0] and resource.activity["historical"]["months_active"] <= description["months_active_range"][1]]
        if KEY == "closed_dollars_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.activity["historical"]["closed_dollars"] >= description["closed_dollars_range"][0] and resource.activity["historical"]["closed_dollars"] <= description["closed_dollars_range"][1]]
        if KEY == "closed_deals_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.activity["historical"]["closed_deals"] >= description["closed_deals_range"][0] and resource.activity["historical"]["closed_deals"] <= description["closed_deals_range"][1]]
        if KEY == "close_ratio_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.close_ratio >= description["close_ratio_range"][0] and resource.close_ratio <= description["close_ratio_range"][1]]
        if KEY == "deals_in_pipeline_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.deals_in_pipeline >= description["deals_in_pipeline_range"][0] and resource.deals_in_pipeline <= description["deals_in_pipeline_range"][1]]
        if KEY == "average_size_of_deal_in_pipeline_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.average_size_of_deal_in_pipeline >= description["average_size_of_deal_in_pipeline_range"][0] and resource.average_size_of_deal_in_pipeline <= description["average_size_of_deal_in_pipeline_range"][1]]
        if KEY == "total_size_of_deals_in_pipeline_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.total_size_of_deals_in_pipeline >= description["total_size_of_deals_in_pipeline_range"][0] and resource.total_size_of_deals_in_pipeline <= description["total_size_of_deals_in_pipeline_range"][1]]
        if KEY == "sales_velocity_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.sales_velocity >= description["sales_velocity_range"][0] and resource.sales_velocity <= description["sales_velocity_range"][1]]
        if KEY == "activation_date_range" and description[KEY] != "*":
            resources = [resource for resource in resources if resource.activation_date >= description["activation_date_range"][0] and resource.activation_date <= description["activation_date_range"][1]]
    return resources

def extract_team(region,country,zone,supervisor_id,resource_id):
    return personnel_search({
        "region":region,
        "country":country,
        "zone":zone,
        "supervisor_id":supervisor_id,
        "resource_id":resource_id
    })

def background_evaluate_resources():
    ENVIRONMENT = read(ENV_FILE)
    resources = ENVIRONMENT["resources"]
    for resource in resources:
        index = ENVIRONMENT["resources"].index(resource)
        # update months_active
        resource.activity["historical"]["months_active"] = int((timestamp() - resource.activation_date).days/30)
        # update has_met_quota
        quota = resource.activity["current"]["quota"]
        quota_starts = resource.activity["historical"]["periods_ended"][-1]
        quota_ends = resource.activity["current"]["period_ends"]
        value_tally = 0; closed_deals = 0; all_deals=0; in_pipeline = 0; pipeline_value =0; closed_deal_dates = []; pipeline_deals_detail = [];
        current_deals = 0; current_cost=0; new_accounts_this_quota=0; value_per_month = 0;
        for account in resource.activity["current"]["accounts"]:
            for lead in resource.activity["current"]["accounts"][account]:
                all_deals+=1
                date_started = lead["new_activity_started"]
                if date_started >= quota_starts and date_started <= quota_ends:
                    if lead["progress"]==8:
                        closed_deals+=1;
                        value_tally+=lead["value"];
                        closed_deal_dates.append(lead["event_detail"][-1]["timestamp"])
                        try:
                            value_per_month+=(lead["value"]/lead["term_months"]);
                        except:
                            pass
                    if lead["progress"] in [5,6,7]:
                        in_pipeline+=1;
                        pipeline_value+=lead["value"];
                        pipeline_deals_detail.append(lead["event_detail"])
                    current_deals+=1;
                    current_cost+=lead["cost"];
                    if lead["progress"]==0:
                        new_accounts_this_quota+=1;
        if value_tally>=quota:
            resource.has_met_quota = True;
        else:
            resource.has_met_quota = False;
        # update close_ratio
        try:
            resource.close_ratio = closed_deals/all_deals;
            # new benchmark?
            if resource.close_ratio > ENVIRONMENT["benchmarks"]["max_close_ratio"][0]:
                ENVIRONMENT["benchmarks"]["max_close_ratio"] = [resource.close_ratio,[resource.resource_id]];
        except:
            resource.close_ratio = 0;
        # update deals_in_pipeline
        resource.deals_in_pipeline = in_pipeline;
        # update total_size_of_deals_in_pipeline
        resource.total_size_of_deals_in_pipeline = pipeline_value;
        # update average_size_of_deal_in_pipeline
        try:
            resource.average_size_of_deal_in_pipeline = pipeline_value/in_pipeline;
        except:
            resource.average_size_of_deal_in_pipeline = 0;
        # update sales_velocity
        active_months = (closed_deal_dates[-1] - closed_deal_dates[0]).days/30;
        try:
            resource.sales_velocity = closed_deals/active_months;
        except:
            resource.sales_velocity = 0;
        # update pipeline deals detail
        resource.pipeline_deals_detail = pipeline_deals_detail;
        # update current deals
        resource.current_deals = current_deals;
        # update current cost
        resource.current_cost = current_cost;
        # new accounts this quota
        resource.new_accounts = new_accounts_this_quota;
        # recurring monthly revenue
        resource.recurring_monthly_revenue = value_per_month;
        ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "background evaluation completed successfully"

def new_resource(resource_id,activation_date,region,country,zone,role,first_name,last_name,date_of_birth,nationality,sex):
    resource = Resource();
    resource.resource_id = resource_id;
    resource.activation_date = activation_date;
    resource.region = region;
    resource.country = country;
    resource.zone = zone;
    resource.role = role;
    resource.first_name = first_name;
    resource.last_name = last_name;
    resource.date_of_birth = date_of_birth;
    resource.nationality = nationality;
    resource.sex = sex;
    ENVIRONMENT = read(ENV_FILE)
    ENVIRONMENT["resources"].append(resource)
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s created successfully" % resource_id

def set_new_quota(resource_id,quota_value,deadline):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["historical"]["quotas"].append(resource.activity["current"]["quota"])
    resource.activity["historical"]["periods_ended"].append(resource.activity["current"]["period_ends"])
    resource.activity["current"]["quota"] = quota_value;
    resource.activity["current"]["period_ends"] = deadline;
    resource.has_met_quota = False;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated successfully" % resource_id

def update_resource_region(resource_id,region):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.region = region
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated successfully" % resource_id

def update_resource_country(resource_id,country):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.country = country
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated successfully" % resource_id

def update_resource_zone(resource_id,zone):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.zone = zone
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated successfully" % resource_id

def update_resource_role(resource_id,role):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.role = role
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated successfully" % resource_id

def update_resource_phone(resource_id,phone):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.company_phone = phone
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated successfully" % resource_id

def update_resource_email(resource_id,email):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.company_email = email
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated successfully" % resource_id

def update_resource_supervisor_id(resource_id,supervisor_id):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.supervisor_id = supervisor_id
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated successfully" % resource_id

def new_lead(lead):
    ENVIRONMENT = read(ENV_FILE);
    ENVIRONMENT["new_leads"].append(lead);
    write(ENV_FILE,ENVIRONMENT)
    return "Info: lead: %s was added successfully" % lead

def new_account(resource_id,account):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    ENVIRONMENT["new_leads"].remove(account)
    resource.activity["current"]["accounts"][account] = [{
        "new_activity_started":timestamp(),
        "progress":0,
        "event_detail":[],
        "value":0,
        "term_months":0,
        "cost":0

    }]
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s added account: %s successfully" % (resource_id,account)

def update_account(resource_id,account):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account].append({
        "new_activity_started":timestamp(),
        "progress":0,
        "event_detail":[],
        "value":0,
        "term_months":0,
        "cost":0

    })
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def new_prospecting_event(resource_id,account,notes,cost):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account][-1]["progress"] = 1;
    resource.activity["current"]["accounts"][account][-1]["event_detail"].append({"timestamp":timestamp(),"notes":notes});
    resource.activity["current"]["accounts"][account][-1]["cost"]+=cost;
    resource.activity_breakdown["prospecting_event"]+=1;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def new_appointment_event(resource_id,account,notes,cost):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account][-1]["progress"] = 2;
    resource.activity["current"]["accounts"][account][-1]["event_detail"].append({"timestamp":timestamp(),"notes":notes});
    resource.activity["current"]["accounts"][account][-1]["cost"]+=cost;
    resource.activity_breakdown["appointment_event"]+=1;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def new_needs_analysis_event(resource_id,account,notes,cost):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account][-1]["progress"] = 3;
    resource.activity["current"]["accounts"][account][-1]["event_detail"].append({"timestamp":timestamp(),"notes":notes});
    resource.activity["current"]["accounts"][account][-1]["cost"]+=cost;
    resource.activity_breakdown["needs_analysis_event"]+=1;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def new_qualified_to_buy_event(resource_id,account,notes,cost):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account][-1]["progress"] = 4;
    resource.activity["current"]["accounts"][account][-1]["event_detail"].append({"timestamp":timestamp(),"notes":notes});
    resource.activity["current"]["accounts"][account][-1]["cost"]+=cost;
    resource.activity_breakdown["qualified_to_buy_event"]+=1;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def new_presentation_scheduled_event(resource_id,account,notes,cost):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account][-1]["progress"] = 5;
    resource.activity["current"]["accounts"][account][-1]["event_detail"].append({"timestamp":timestamp(),"notes":notes});
    resource.activity["current"]["accounts"][account][-1]["cost"]+=cost;
    resource.activity_breakdown["presentation_scheduled_event"]+=1;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def new_decision_maker_buy_in_event(resource_id,account,notes,cost):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account][-1]["progress"] = 6;
    resource.activity["current"]["accounts"][account][-1]["event_detail"].append({"timestamp":timestamp(),"notes":notes});
    resource.activity["current"]["accounts"][account][-1]["cost"]+=cost;
    resource.activity_breakdown["decision_maker_buy_in_event"]+=1;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def new_contract_sent_event(resource_id,account,notes,contract_value,contract_term_months,cost):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account][-1]["progress"] = 7;
    resource.activity["current"]["accounts"][account][-1]["value"] = contract_value;
    resource.activity["current"]["accounts"][account][-1]["term_months"] = contract_term_months;
    resource.activity["current"]["accounts"][account][-1]["event_detail"].append({"timestamp":timestamp(),"notes":notes});
    resource.activity["current"]["accounts"][account][-1]["cost"]+=cost;
    resource.activity_breakdown["contract_sent_event"]+=1;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def new_closed_won_event(resource_id,account,notes):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account][-1]["progress"] = 8;
    resource.activity["historical"]["closed_deals"]+=1;
    resource.activity["historical"]["closed_dollars"]+=resource.activity["current"]["accounts"][account][-1]["value"];
    resource.activity["current"]["accounts"][account][-1]["event_detail"].append({"timestamp":timestamp(),"notes":notes});
    resource.activity_breakdown["closed_won_event"]+=1;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def new_closed_lost_event(resource_id,account,notes):
    resource = personnel_search({"resource_id":resource_id})[0]
    ENVIRONMENT = read(ENV_FILE)
    index = ENVIRONMENT["resources"].index(resource)
    resource.activity["current"]["accounts"][account][-1]["progress"] = -1;
    resource.activity["historical"]["lost_deals"]+=1;
    resource.activity["historical"]["lost_dollars"]+=resource.activity["current"]["accounts"][account][-1]["value"];
    resource.activity["current"]["accounts"][account][-1]["event_detail"].append({"timestamp":timestamp(),"notes":notes});
    resource.activity_breakdown["closed_lost_event"]+=1;
    ENVIRONMENT["resources"][index] = resource
    write(ENV_FILE,ENVIRONMENT)
    return "Info: resource with id: %s updated account: %s successfully" % (resource_id,account)

def KPI_group_account_insight(resources):
    ENVIRONMENT = read(ENV_FILE);
    report = {
    "customer_retention_rate":0,
    "customer_churn_rate":0,
    "projected_growth_rate":0,
    "annual_recurring_revenue":0,
    "monthly_recurring_revenue":0,
    "annual_contract_value":0
    }
    retained_deals = 0; lost_deals=0; new_deals=0; sum_monthly_recurring=0;
    for resource in resources:
        retained_deals+=resource.activity["historical"]["closed_deals"];
        lost_deals+=resource.activity["historical"]["lost_deals"];
        new_deals+=resource.new_accounts;
        sum_monthly_recurring+=resource.recurring_monthly_revenue;
    report["monthly_recurring_revenue"] = sum_monthly_recurring;
    report["annual_recurring_revenue"] = sum_monthly_recurring*12;
    report["annual_contract_value"] = sum_monthly_recurring*12;
    if report["monthly_recurring_revenue"] > ENVIRONMENT["benchmarks"]["monthly_recurring_revenue"][0]:
        ENVIRONMENT["benchmarks"]["monthly_recurring_revenue"] = [report["monthly_recurring_revenue"],map(lambda x:x.resource_id,resources)];
    if report["annual_recurring_revenue"] > ENVIRONMENT["benchmarks"]["annual_recurring_revenue"][0]:
        ENVIRONMENT["benchmarks"]["annual_recurring_revenue"] = [report["annual_recurring_revenue"],map(lambda x:x.resource_id,resources)];
    if report["annual_contract_value"] > ENVIRONMENT["benchmarks"]["annual_contract_value"][0]:
        ENVIRONMENT["benchmarks"]["annual_contract_value"] = [report["annual_contract_value"],map(lambda x:x.resource_id,resources)];
    sum_deals = retained_deals+lost_deals+new_deals;
    if sum_deals>0:
        report["customer_retention_rate"] = retained_deals/sum_deals;
        report["projected_growth_rate"] = new_deals/sum_deals;
        report["customer_churn_rate"] = lost_deals/sum_deals;
        if report["customer_retention_rate"] > ENVIRONMENT["benchmarks"]["customer_retention_rate"][0]:
            ENVIRONMENT["benchmarks"]["customer_retention_rate"] = [report["customer_retention_rate"],map(lambda x:x.resource_id,resources)];
        if report["projected_growth_rate"] > ENVIRONMENT["benchmarks"]["projected_growth_rate"][0]:
            ENVIRONMENT["benchmarks"]["projected_growth_rate"] = [report["projected_growth_rate"],map(lambda x:x.resource_id,resources)];
        if report["customer_churn_rate"] > ENVIRONMENT["benchmarks"]["customer_churn_rate"][0]:
            ENVIRONMENT["benchmarks"]["customer_churn_rate"] = [report["customer_churn_rate"],map(lambda x:x.resource_id,resources)];
    write(ENV_FILE,ENVIRONMENT)
    return report

def KPI_group_resource_utilization(resources):
    ENVIRONMENT = read(ENV_FILE);
    report = {
    "deals_in_pipeline":0,
    "deals_detail":{},
    "pipeline_value":0,
    "pipeline_coverage_ratio":0,
    "win_loss_ratio":0,
    "prospecting_events":0,
    "appointment_events":0,
    "needs_analysis_events":0,
    "qualified_to_buy_events":0,
    "presentation_scheduled_events":0,
    "decision_maker_buy_in_events":0,
    "contract_sent_events":0,
    "closed_won_events":0,
    "closed_lost_events":0,
    "total_accounts":0,
    "gross_revenue":0,
    "revenue_per_head":0,
    "total_cost":0,
    "gross_margin":0
    }
    all_current_deals = 0; all_pipeline_deals = 0; sum_close_ratio = 0; sum_revenue=0; sum_cost=0;
    for resource in resources:
        all_current_deals+=resource.current_deals;
        all_pipeline_deals+=resource.deals_in_pipeline;
        sum_close_ratio+=resource.close_ratio;
        sum_revenue+=resource.activity["historical"]["closed_dollars"];
        sum_cost+=resource.current_cost;
        report["deals_in_pipeline"]+=resource.deals_in_pipeline;
        report["deals_detail"][resource.resource_id] = resource.pipeline_deals_detail;
        report["pipeline_value"]+=resource.total_size_of_deals_in_pipeline;
        report["prospecting_events"]+=resource.activity_breakdown["prospecting_event"];
        report["appointment_events"]+=resource.activity_breakdown["appointment_event"];
        report["needs_analysis_events"]+=resource.activity_breakdown["needs_analysis_event"];
        report["qualified_to_buy_events"]+=resource.activity_breakdown["qualified_to_buy_event"];
        report["presentation_scheduled_events"]+=resource.activity_breakdown["presentation_scheduled_event"];
        report["decision_maker_buy_in_events"]+=resource.activity_breakdown["decision_maker_buy_in_event"];
        report["contract_sent_events"]+=resource.activity_breakdown["contract_sent_event"];
        report["closed_won_events"]+=resource.activity_breakdown["closed_won_event"];
        report["closed_lost_events"]+=resource.activity_breakdown["closed_lost_event"];
        report["total_accounts"]+=len(resource.activity["current"]["accounts"]);
    report["gross_revenue"] = sum_revenue;
    if report["gross_revenue"] > ENVIRONMENT["benchmarks"]["gross_revenue"][0]:
        ENVIRONMENT["benchmarks"]["gross_revenue"] = [report["gross_revenue"],map(lambda x:x.resource_id,resources)];
    report["total_cost"] = sum_cost;
    if report["total_cost"] > ENVIRONMENT["benchmarks"]["total_cost"][0]:
        ENVIRONMENT["benchmarks"]["total_cost"] = [report["total_cost"],map(lambda x:x.resource_id,resources)];
    if sum_revenue > 0:
        report["gross_margin"] = 1-sum_cost/sum_revenue;
        if report["gross_margin"] > ENVIRONMENT["benchmarks"]["gross_margin"][0]:
            ENVIRONMENT["benchmarks"]["gross_margin"] = [report["gross_margin"],map(lambda x:x.resource_id,resources)];
    if all_current_deals > 0:
        report["pipeline_coverage_ratio"] = all_pipeline_deals/all_current_deals;
    if len(resources)> 0:
        report["win_loss_ratio"] = sum_close_ratio/len(resources);
        report["revenue_per_head"] = sum_revenue/len(resources);
        if report["revenue_per_head"] > ENVIRONMENT["benchmarks"]["revenue_per_head"][0]:
            ENVIRONMENT["benchmarks"]["revenue_per_head"] = [report["revenue_per_head"],map(lambda x:x.resource_id,resources)];
    write(ENV_FILE,ENVIRONMENT)
    return report

def KPI_individual_resource_utilization(resource_id):
    return KPI_group_resource_utilization(extract_team("*","*","*","*",resource_id));

def KPI_team_resource_utilization(region,country,zone,supervisor_id):
    return KPI_group_resource_utilization(extract_team(region,country,zone,supervisor_id,"*"));

def KPI_individual_account_insight(resource_id):
    return KPI_group_account_insight(extract_team("*","*","*","*",resource_id));

def KPI_team_account_insight(region,country,zone,supervisor_id):
    return KPI_group_account_insight(extract_team(region,country,zone,supervisor_id,"*"));
