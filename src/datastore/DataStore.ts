import { Execution } from '../engine/Execution';
import { IDataStore, IBPMNServer, IInstanceData, IItemData } from '../interfaces';

import { ServerComponent } from '../server/ServerComponent';


import { InstanceLocker } from './';

import { QueryTranslator } from './QueryTranslator';

const fs = require('fs');

const MongoDB = require('./MongoDB').MongoDB;


const Instance_collection = 'wf_instances';
const Locks_collection = 'wf_locks';
const Events_collection = 'wf_events';


class DataStore extends ServerComponent  implements IDataStore {

	dbConfiguration;
	db;

	execution: Execution;
	isModified = false;
	isRunning = false;
	inSaving = false;
	promises = [];
	locker;

	constructor(server: IBPMNServer) {
		super(server);

		this.dbConfiguration = this.configuration.database.MongoDB;
		this.db = new MongoDB(this.dbConfiguration, this.logger);
		this.locker=new InstanceLocker(this);

	}
	/*monitorExecution(execution: Execution) {
		this.execution = execution;
		const listener = execution.listener;
	} */

	async save(instance,options={}) {
		return await this.saveInstance(instance);
	}
	async loadInstance(instanceId,options={}) {

		const recs = await this.findInstances({ id: instanceId }, 'full');
		if (recs.length == 0) {

			this.logger.error("Instance is not found for this item");
			return null;
		}
		const instanceData = recs[0];

//		this.logger.log(" instance obj found" + instanceData.id);

		return { instance: instanceData, items: this.getItemsFromInstances([instanceData]) };
	}
	/*
	Since MongoDB returns the wholde doc (all items), we have to filter only what user asked for 
	and transform the data 
*/
	private getItemsFromInstances(instances, condition = null,trans=null) {
		const items = [];
		instances.forEach(instance => {
			instance.items.forEach(i => {
				let pass = true;
				if (trans)
					pass = trans.filterItem(i, condition);

				if (pass) {
					i['processName'] = instance.name;
					i['data'] = instance.data;
					i['instanceId'] = instance.id;
					i['instanceVersion'] = instance.version;
					items.push(i);
				}
			});
		});
		return items.sort(function (a, b) { return (a.seq - b.seq); });
	}


	// save instance to DB
	static seq = 0;
	private async saveInstance(instance,options={}) {
//		this.logger.log("Saving...");


		//var json = JSON.stringify(instance.state, null, 2);
		const tokensCount = instance.tokens.length;
		let itemsCount = instance.items.length;

		var recs;
		if (!instance.saved) {
			instance.saved = new Date();
			instance.version =0;

			//this.promises.push(this.db.insert(this.dbConfiguration.db, Instance_collection, [instance]));
			//this.promises.push(this.db.insert(this.dbConfiguration.db, Instance_collection, [instance]));
			await this.db.insert(this.dbConfiguration.db, Instance_collection, [instance]);

//			this.logger.log("inserting instance");
		}
		else {
			this.promises.push(this.db.update(this.dbConfiguration.db, Instance_collection,
				{ id: instance.id },
				{
					$set:
					{	version: instance.version,endedAt: instance.endedAt, status: instance.status, saved: instance.saved,
						tokens: instance.tokens, items: instance.items, loops: instance.loops,
						logs: instance.logs, data: instance.data
					}
				}));

//			this.logger.log("updating instance");
		}
		/*t fileName = instance.name + '_' + DataStore.seq++ + '.state';
		await fs.writeFile(fileName, JSON.stringify(instance), function (err) {
			if (err) throw err;
		});*/

		await Promise.all(this.promises);
		this.logger.log('..DataStore:saving Complete');

	}

	async findItem(query): Promise <IItemData> {
		let results = await this.findItems(query);
		if (results.length == 0)
			throw Error(" No items found for " + JSON.stringify(query));
		else if (results.length > 1)
			throw Error(" More than one record found " + results.length + JSON.stringify(query));
		else
			return results[0];

	}
	async findInstance(query, options): Promise<IInstanceData> {

		let results = await this.findInstances(query, options);
		if (results.length == 0)
			throw Error(" No instance found for " + JSON.stringify(query));
		else if (results.length > 1)
			throw Error(" More than one record found " + results.length + JSON.stringify(query));

		const rec = results[0];

		/*this.convertColl(rec.authorizations, Authorization);
		this.convertColl(rec.involvements, Involvement);
		rec.items.forEach(item => {
			this.convertColl(item.authorizations, Authorization);
			this.convertColl(item.assignments, Assignment);
			this.convertColl(item.notifications, Notification);

		}); */
		return rec;

	}
	convertObj(obj, cls) {
		return Object.assign(new cls,obj);

    }
	convertColl(coll, cls) {
		if (coll)
		{
		for (let i = 0; i < coll.length; i++) {
			const el = coll[i];
			coll[i] = Object.assign(new cls, el);
			}
		}

    }
	async findInstances(query, option: 'summary' | 'full' | any = 'summary'): Promise<IInstanceData[]>{

		let projection;

		if (option == 'summary')
			projection = { source: 0, logs: 0 };
		else
			projection = {};

		var records = await this.db.find(this.dbConfiguration.db, Instance_collection, query, projection);
		return records;
	}
	/**
		* scenario:
		*
		* ```
		* itemId			{ items { id : value } }
		* itemKey			{ items {key: value } }
		* instance, task	{ instance: { id: instanceId }, items: { elementId: value }}
		* message			{ items: { messageId: nameofmessage, key: value } {}
		* status			{ items: {status: 'wait' } }
		* custom: { query: query, projection: projection }
		* ```
		* New approach:
		* just like MongoDB
		* ```
		* itemId			{ items { id : value } }
		* itemKey			{ items {key: value } }
		* instance, task	{  id: instanceId , items.elementId: value }
		* message			{ items.messageId: nameofmessage, key: value } {}
		* status			{ items.status: 'wait' } }
		* custom: { query: query, projection: projection }
		* ```

		* Problem with Mongodb:	`projection $elematch returns only the first record`
	 *
	 * @param query
	 */
	async findItems(query) : Promise<IItemData[]> {
		// let us rebuild the query form {status: value} to >  "tokens.items.status": "wait" 
		const trans = new QueryTranslator('items');
		const result = trans.translateCriteria(query);
		const projection = { id: 1, data: 1, name: 1, "items": 1 };
		var records = await this.db.find(this.dbConfiguration.db, Instance_collection, result, projection);
		// console.log('...find items for query:', query, " translated to :", JSON.stringify(result),  " recs:" , records.length)

		const items=this.getItemsFromInstances(records, result,trans);
		this.logger.log('...find items for ' + JSON.stringify(query) + " result :" + JSON.stringify(result) + " instances:" + records.length+ " items: "+items.length);
		return items;
	}


	async deleteInstances(query) {

		await this.cache.shutdown();
		return await this.db.remove(this.dbConfiguration.db, Instance_collection, query );

	}
	// db.collection.createIndex({ "a.loc": 1, "a.qty": 1 }, { unique: true })
    /**
     * first time installation of DB
     * 
     * creates a new collection and add an index
     * 
     * */
	async install() {
		await this.db.createIndex(this.dbConfiguration.db, Instance_collection, { id: 1 }, { unique: true });
		await this.db.createIndex(this.dbConfiguration.db, Instance_collection, { "items.id": 1 });
		await this.db.createIndex(this.dbConfiguration.db, Locks_collection, { id: 1 }, { unique: true });
	}
	// LOCKS
}

export { DataStore };