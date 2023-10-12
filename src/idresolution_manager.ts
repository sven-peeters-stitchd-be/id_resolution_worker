import { getTokenFromGCPServiceAccount } from '@sagi.io/workers-jwt';

export class IDResolutionManager {

    private bq_account_json;
    private audience : string = 'https://bigquery.googleapis.com/';
    private hardid : string = "";
    private email : string = "";
    private projectName : string = "";
    private datasetName : string = "mockup_idresolution";
    

    constructor(account_json : any) {
        this.bq_account_json = account_json;
        this.projectName = account_json.project_id;
    }

    
    private async getJwtToken() : Promise<string> {
        
        return await getTokenFromGCPServiceAccount( { serviceAccountJSON : this.bq_account_json , aud : this.audience } );
    }

    public async ProcessRequest(request : IDResolutionManagerRequest) : Promise<IDResolutionManagerResponse> {

        try {

            

            switch (request.Operation.toUpperCase()) {
                case "GETIDLIST":
                    return this.getIdList();                    
                case "UPDATE":
                    return this.upsertIdentity(request);
                case "PURGE":
                    return this.purgeDatabase();
                default:
                    return new IDResolutionManagerResponse(this.hardid,false,"Unsupported operation",null);
            }
            
        } catch (error : any) {
            return new IDResolutionManagerResponse(this.hardid,false,"Unhandled exception : " + error,null);
        }

        

    }

    private async getIdList() {

        try {

            let response = await this.runSQLQuery("SELECT * FROM " + this.datasetName + ".id_types");
            return new IDResolutionManagerResponse(this.hardid,true,"",response);
            
        } catch (error) {
            return new IDResolutionManagerResponse(this.hardid,false,"Unable to get id list : " + error,null);
        }
        
    }

    private async upsertIdentity(request : IDResolutionManagerRequest) : Promise<IDResolutionManagerResponse> {

        try {

            //Fetch current hardid if passed along            
            await this.fetchCurrentHardId(request);
            if (this.hardid) {
                // Found hard id, check if it's a different one or not
                if (request.HardId && this.hardid !== request.HardId) {
                    await this.reAssignHardId(this.hardid,request.HardId);
                }
            }
            await this.upsertHardId(request.Email);            

            // Process all soft id's
            if ( request.IdList) {
                for (let index = 0; index < request.IdList.length; index++) {
                    await this.upsertSoftId(this.hardid,request.IdList[index].IdType,request.IdList[index].Value);                
                }
            }

            return new IDResolutionManagerResponse(this.hardid,true,"",null);
            
        } catch (error) {
            return new IDResolutionManagerResponse(this.hardid,false,"Unable to upsert : " + error,null);
        }        

    }

    private async purgeDatabase() : Promise<IDResolutionManagerResponse> {

        try {

            await this.runSQLQuery("TRUNCATE TABLE " + this.datasetName + ".hard_id_list");
            await this.runSQLQuery("TRUNCATE TABLE " + this.datasetName + ".soft_id_list");

            return new IDResolutionManagerResponse(this.hardid,true,"",null);
            
        } catch (error) {
            return new IDResolutionManagerResponse(this.hardid,false,"Unable to purge : " + error,null);
        }
        
    }

    private async fetchCurrentHardId(request : IDResolutionManagerRequest) {

        if (request.Email) {
            // Check if we can find the email adres
            const queryResultEmail = await this.runSQLQuery("SELECT hard_id_list_id,email FROM " + this.datasetName + ".hard_id_list where email = '" + request.Email + "'")
            if (queryResultEmail && queryResultEmail.length > 0) {
                this.hardid = queryResultEmail[0].hard_id_list_id;
                this.email = queryResultEmail[0].email;

                if (!request.HardId) {
                    // Merge all possible softid's to this new hard id

                    let processedHardIds: string [] = [this.hardid];

                    if (request.IdList && request.IdList.length > 0) {
                        // Check for softid's
                        for (let index = 0; index < request.IdList.length; index++) {
                            const queryResultSoftId= await this.runSQLQuery("SELECT hard_id_list_id FROM " + this.datasetName + ".soft_id_list where id_type = " + request.IdList[index].IdType + " and id_value = '" + request.IdList[index].Value + "'");
                            for (let subIndex = 0; subIndex < queryResultSoftId.length; subIndex++) {
                                if (!processedHardIds.includes(queryResultSoftId[subIndex].hard_id_list_id)) {
                                    await this.reAssignHardId(this.hardid,queryResultSoftId[subIndex].hard_id_list_id);
                                    processedHardIds.push(queryResultSoftId[subIndex].hard_id_list_id);
                                }
                            }
                            
                        }
                    }
                }

                return;
            }
        } 
        
        if (request.HardId) {            
            // First check for hardid match
            const queryResultHardIdDirect = await this.runSQLQuery("SELECT hard_id_list_id,email FROM " + this.datasetName + ".hard_id_list where hard_id_list_id = '" + request.HardId + "'")
            if (queryResultHardIdDirect && queryResultHardIdDirect.length == 1) {
                this.hardid = queryResultHardIdDirect[0].hard_id_list_id;
                this.email = queryResultHardIdDirect[0].email;
                return;
            } else {
            // Then check if it's a hold hardid
                const queryResultHardIdInDirect = await this.runSQLQuery("SELECT hard_id_list_id FROM " + this.datasetName + ".soft_id_list where id_type = 0 and id_value = '" + request.HardId + "'");
                if (queryResultHardIdInDirect && queryResultHardIdInDirect.length > 1) {
                    // Old hardid so switching to new hardid
                    let newRequest = new IDResolutionManagerRequest();
                    newRequest.HardId = queryResultHardIdInDirect[0].hard_id_list_id;
                    await this.fetchCurrentHardId(newRequest)
                    return;
                }
            }       
        }
        
        if (request.IdList && request.IdList.length > 0) {
            // Check for softid's
            for (let index = 0; index < request.IdList.length; index++) {
                const queryResultSoftId= await this.runSQLQuery("SELECT hard_id_list_id FROM " + this.datasetName + ".soft_id_list where id_type = " + request.IdList[index].IdType + " and id_value = '" + request.IdList[index].Value + "'");
                if (queryResultSoftId && queryResultSoftId.length > 0) {
                    let newRequest = new IDResolutionManagerRequest();
                    newRequest.HardId = queryResultSoftId[0].hard_id_list_id;
                    await this.fetchCurrentHardId(newRequest)
                    return;
                }
                
            }
        }

    }

    private async upsertHardId(email : string) {

        if (email == undefined || email == null)
            email = "";

        if (this.hardid) {
            if (email && (!this.email || this.email.toUpperCase() !== email.toUpperCase())) {
                // Update email on existing hardid
                await this.runSQLQuery("UPDATE " + this.datasetName + ".hard_id_list set email = '" + email + "' where hard_id_list_id = '" + this.hardid + "'");
            }
            return;
        }
            
        // create new hardid
        const newHardId : string = this.generateUUIDv4();
        await this.runSQLQuery("INSERT INTO " + this.datasetName + ".hard_id_list(hard_id_list_id,email) SELECT '" + newHardId + "','" + email + "'");
        this.hardid = newHardId;
    }

    private async upsertSoftId(hardid : string,idtype : number,idvalue : string) {

        await this.runSQLQuery("IF NOT EXISTS(SELECT soft_id_list_id FROM " + this.datasetName + ".soft_id_list WHERE hard_id_list_id = '" + hardid + "' AND id_type = " + idtype + " and id_value = '" + idvalue + "') THEN INSERT INTO " + this.datasetName + ".soft_id_list(soft_id_list_id,id_type,id_value,hard_id_list_id) SELECT '" + this.generateUUIDv4() + "'," + idtype + ",'" + idvalue + "','" + hardid + "';END IF;");

    }

    private async reAssignHardId(newHardid : string,oldHardId : string) {
        
        await this.runSQLQuery("UPDATE " + this.datasetName + ".soft_id_list set hard_id_list_id = '" + newHardid + "' where id_type != 0 and hard_id_list_id = '" + oldHardId + "'");
        await this.runSQLQuery("INSERT INTO " + this.datasetName + ".soft_id_list(soft_id_list_id,id_type,id_value,hard_id_list_id) SELECT '" + this.generateUUIDv4() + "',0,'" + oldHardId + "','" + newHardid + "'");
        await this.runSQLQuery("DELETE FROM " + this.datasetName + ".hard_id_list where hard_id_list_id = '" + oldHardId + "'");

    }

    private async runSQLQuery(sql : string) : Promise<any> {

        const queryUrl = "https://bigquery.googleapis.com/bigquery/v2/projects/" + this.bq_account_json.project_id + "/queries";
        const payload = {
            'kind' : 'bigquery#queryRequest',
            'query' : sql,
            'useLegacySql' : false
        };

        const init = {
            body: JSON.stringify(payload),
            method: "POST",
            headers: {
              "content-type": "application/json;charset=UTF-8",
                "Authorization": "Bearer " + await this.getJwtToken()
            },
          };

        const queryResult = await fetch(queryUrl,init);
        const queryResultObject : QueryResult = await queryResult.json();

        if (queryResultObject.error)
          throw new Error(queryResultObject.error.message);
          

        if (queryResultObject.rows)
            return this.convertToTypedDataSet(queryResultObject);
        else
            return null;

    }

    private convertToTypedDataSet(queryResult : QueryResult) : any {

        let returnObject : any[] = [];
        queryResult.rows.forEach(row => {
            let rowObject : Record<string,string> = {};
            let currentField : number = 0;
            queryResult.schema.fields.forEach(field => {
                rowObject[field.name] = row.f[currentField].v;
                currentField++;
            });
            returnObject.push(rowObject);
        });

        return returnObject;

    }

    private generateUUIDv4() { 
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
        .replace(/[xy]/g, function (c) { 
            const r = Math.random() * 16 | 0,  
                v = c == 'x' ? r : (r & 0x3 | 0x8); 
            return v.toString(16); 
        }); 
    }

}

export class IDResolutionManagerRequestIdValue {
    public IdType : number = 0;
    public Value: string = "";
}

export class IDResolutionManagerRequest {

    public Operation : string = "";
    public HardId : string = "";
    public Email : string = "";
    public IdList : IDResolutionManagerRequestIdValue[] = [] ;

}

export class IDResolutionManagerResponse {

    public HardId : string = "";
    public Success : boolean = false;
    public ErrorMessage : string = "";
    public QueryResult : any;

    constructor(hardid : string,success : boolean, errormessage : string, queryresult : any)
    {
        this.HardId = hardid;
        this.Success = success;
        this.ErrorMessage = errormessage;
        this.QueryResult = queryresult;
    }
}

interface QueryResult {
    kind: string
    schema: QueryResultSchema
    totalRows: string
    rows: QueryResultRow[]
    totalBytesProcessed: string
    jobComplete: boolean
    cacheHit: boolean
    error: any
  }
  
interface QueryResultSchema {
    fields: QueryResultField[]
  }
  
interface QueryResultField {
    name: string
    type: string    
  }

  
interface QueryResultRow {
    f: QueryResultRowValue[]
  }
  
interface QueryResultRowValue {
    v: string
  }