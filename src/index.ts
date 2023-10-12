/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

import { getTokenFromGCPServiceAccount } from '@sagi.io/workers-jwt';
import {IDResolutionManager,IDResolutionManagerRequest,IDResolutionManagerRequestIdValue}  from './idresolution_manager'

export interface Env {
	BQ_ACCOUNT_TYPE:  string;
	BQ_PROJECTID: string;
	BQ_AUTH_URI: string;
	BQ_TOKEN_URI: string;
	BQ_AUTH_X509_URI: string;
	BQ_UNIVERSE_DOMAIN: string;
	BQ_PRIVATEKEY_ID: string;
	BQ_PRIVATEKEY_CONTENT: string;
	BQ_CLIENT_EMAIL: string;
	BQ_CLIENT_ID: string;
	BQ_CLIENT_X509_URI: string;

	// Example binding to KV. Learn more at https://developers.cloudflare.com/workers/runtime-apis/kv/
	// MY_KV_NAMESPACE: KVNamespace;
	//
	// Example binding to Durable Object. Learn more at https://developers.cloudflare.com/workers/runtime-apis/durable-objects/
	// MY_DURABLE_OBJECT: DurableObjectNamespace;
	//
	// Example binding to R2. Learn more at https://developers.cloudflare.com/workers/runtime-apis/r2/
	// MY_BUCKET: R2Bucket;
	//
	// Example binding to a Service. Learn more at https://developers.cloudflare.com/workers/runtime-apis/service-bindings/
	// MY_SERVICE: Fetcher;
	//
	// Example binding to a Queue. Learn more at https://developers.cloudflare.com/queues/javascript-apis/
	// MY_QUEUE: Queue;
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {

		if (request.method !== "POST")
			return new Response("Only POST method is supported");

		const contentType = request.headers.get("content-type") || "";
		if (!contentType.includes("application/json")) 
			return new Response("JSON body expected");

		const serviceAccountJSON = {
			'type': env.BQ_ACCOUNT_TYPE,
			'project_id': env.BQ_PROJECTID,
			'private_key_id': env.BQ_PRIVATEKEY_ID,
			'private_key': env.BQ_PRIVATEKEY_CONTENT.split(String.raw`\n`).join('\n'),
			'client_email': env.BQ_CLIENT_EMAIL,
			'client_id': env.BQ_CLIENT_ID,
			'auth_uri': env.BQ_AUTH_URI,
			'token_uri': env.BQ_TOKEN_URI,
			'auth_provider_x509_cert_url': env.BQ_AUTH_X509_URI,
			'client_x509_cert_url': env.BQ_CLIENT_X509_URI,
			'universe_domain': env.BQ_UNIVERSE_DOMAIN
		  };

  		let manager = new IDResolutionManager(serviceAccountJSON)
		return new Response(JSON.stringify(await manager.ProcessRequest(await request.json())),
		{
			headers: {
			  "content-type": "application/json;charset=UTF-8",
			}
		});
		
	},
};
