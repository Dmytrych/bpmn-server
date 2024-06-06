
import {Execution, EXECUTION_STATUS, IBPMNServer, IInstanceData, IMigrator, Item, Loop, Token} from '../';
import { ServerComponent } from './ServerComponent';
import { EXECUTION_EVENT, IEngine} from "../interfaces";

const { v4: uuidv4 } = require('uuid');

class Engine extends ServerComponent implements IEngine{

	runningCounter=0;
	callsCounter=0;
	constructor(server: IBPMNServer) {
		super(server);
  }

	/**
	 *	loads a definitions  and start execution
	 *
	 * @param name		name of the process to start
	 * @param data		input data 
	 * @param startNodeId	in process has multiple start node; you need to specify which one
	 */
	async start(name: any,
		data: any = {}, 
		startNodeId: string = null,
		userName: string=null,
		options = {}): Promise<Execution> {
		this.runningCounter++;
		this.logger.log(`^Action:engine.start ${name}`);
		

		const definitions = this.definitions;
		const source = await definitions.getSource(name);

		const execution = new Execution(this.server,name, source);

		if (options['parentItemId']) {
			execution.instance.parentItemId=options['parentItemId'];
		}
		execution.userName = userName;
		execution.operation='start';
		execution.options=options;
	
		this.cache.add(execution);

		try {
			await this.lock(execution.id);
			execution.isLocked = true;


			if (options['noWait'] == true) {
				execution.worker = execution.execute(startNodeId, this.sanitizeData(data), options);
				execution.worker.then(obj=>{ 
						this.logger.log('after worker is done releasing ..'+execution.instance.id);
						this.release(execution);
						});
				return execution;
			}
			else {
				const waiter = await execution.execute(startNodeId, this.sanitizeData(data), options);
				await this.release(execution);
				this.logger.log(`.engine.start ended for ${name}`);
				return execution;
			}
		}
		catch(exc) {
			return await this.exception(exc,execution); 
		}
		finally {
			this.runningCounter--;
			if (execution && execution.isLocked)
				await this.release(execution);
		}
		
	}

	public async restart(itemQuery, data:any,userName, options={}) :Promise<Execution>  {
	
		this.logger.log(`^Action:engine.restart`);
		let execution;
		this.runningCounter++;
		this.callsCounter++;

		try {
			const item = await this.server.dataStore.findItem(itemQuery);

			const instance = await this.server.dataStore.findInstance({id:item.instanceId});

			execution= await this.restore(instance.id,item.id);

			await execution.restart(item.id, this.sanitizeData(data),userName, options);

			await this.release(execution);

			return execution;
		}
		catch (exc) {
			return await this.exception(exc,execution); 
		}
		finally {
			this.runningCounter--;
			if (execution && execution.isLocked)
				await this.release(execution);
		}
		
			
	}

  public async migrate(instanceId: string, targetDefinitionName: string): Promise<Execution> {
    let execution;

    const definitions = this.definitions;
    const source = await definitions.getSource(targetDefinitionName);

    try {
      const instance = await this.server.dataStore.findInstance({ id: instanceId });

      execution = await this.restore(instance.id);
      const preMigrationState = execution.getState();
      const executionItems = await execution.getItems();

      console.log(`Original execution has ${executionItems.length} items`)

      const { version, name, startedAt, endedAt, data, parentItemId } = preMigrationState;
      const newId = uuidv4();

      const migratedInfo = {
        id: newId,
        items: executionItems.map((item) => item.save()), // converting it to DB entity
        name: targetDefinitionName,
        version,
        startedAt,
        endedAt,
        status: EXECUTION_STATUS.running,
        saved: false,
        data,
        source,
        parentItemId,
        logs: [],
        tokens: [],
        loops: []
      }

      console.log(`The target will have ${migratedInfo.items.length} items`)

      const clonedExecution = new Execution(this.server, targetDefinitionName, source, migratedInfo)

      console.log("Created a new execution")

      const migrator = new InstanceMigrator(this.server);
      await migrator.migrate(execution, clonedExecution)

      console.log("Migrated. Saving...")

      await clonedExecution.save()

      console.log("Execution saved")

      await this.release(execution);

      return execution;
    }
    catch (exc) {
      return await this.exception(exc,execution);
    }
    finally {
      if (execution && execution.isLocked)
        await this.release(execution);
    }

    // console.log(instance.definition.nodes)

    // await bpmnServer.engine.restart({ id: "5680d053-448d-4ac7-9058-994561c520f0", "items.id": "78712349-8994-4663-b4a9-554925aea690" }, { }, "user1")
    //
    // const { logs, ...state} = instance.getState();
    // console.log(state)
    // const a = await Execution.getCopy(bpmnServer, { ...state, id: "testaaaa", _id: undefined, logs: [], saved: false } as unknown as IInstanceData)
    //
    // this.logger.log(``);
    //
    //
    // const definitions = this.definitions;
    // const source = await definitions.getSource(name);
    //
    // const execution = new Execution(this.server,name, source);
    // execution.userName = userName;
    // execution.operation='start';
    // execution.options=options;
    //
    // this.cache.add(execution);
    //
    // try {
    //   await this.lock(execution.id);
    //   execution.isLocked = true;
    //
    //
    //   if (options['noWait'] == true) {
    //     execution.worker = execution.execute(startNodeId, this.sanitizeData(data), options);
    //     execution.worker.then(obj=>{
    //       this.logger.log('after worker is done releasing ..'+execution.instance.id);
    //       this.release(execution);
    //     });
    //     return execution;
    //   }
    //   else {
    //     const waiter = await execution.execute(startNodeId, this.sanitizeData(data), options);
    //     await this.release(execution);
    //     this.logger.log(`.engine.start ended for ${name}`);
    //     return execution;
    //   }
    // }
    // catch(exc) {
    //   return await this.exception(exc,execution);
    // }
    // finally {
    //   if (execution && execution.isLocked)
    //     await this.release(execution);
    // }

  }
	
	
	/**
	 * restores an instance into memeory or provides you access to a running instance
	 * 
	 * this will also resume execution
	 * 
		* @param instanceQuery		criteria to fetch the instance
		*
		* query example:
		* 
		* ```jsonl
		* { id: instanceId}
		* { data: {caseId: 1005}}
		* { items.id : 'abcc111322'}
		* { items.itemKey : 'businesskey here'}
		* ```
	 */
	async get(instanceQuery): Promise<Execution> {

		let instance = await this.dataStore.findInstance(instanceQuery);
		const execution = await this.restore(instance.id);
		await this.release(execution);
		return execution;
	}
	/**
		lock instance 
	*/
	private async lock(executionId) {
			this.logger.log('...locking ..'+executionId);
			await this.server.dataStore.locker.lock(executionId);
			
			this.logger.log('   locking complete' + executionId);
	}
	/**
		release instance lock
	*/
	private async release(execution: Execution) {
		this.logger.log('...unlocking ..' + execution.id);
			await this.server.dataStore.locker.release(execution.id);
			execution.isLocked=false;
	}
	/***
		Loads instance into memory for purpose of execution
		Locks instance first if required
		check if in cache
	*/
	/*static restorePromise = null;
	private async restore(instanceId): Promise<Execution> {

		if (Engine.restorePromise)
			await Engine.restorePromise;

		Engine.restorePromise = this.doRestore(instanceId);

		let ret=await Engine.restorePromise;

		Engine.restorePromise = null;
		return ret;
	}
	 */
	private async restore(instanceId,itemId=null): Promise<Execution> {

		// need to load instance first
		let execution;

		await this.lock(instanceId);	// if fails throws exception

		let instance = await this.dataStore.findInstance({ id: instanceId }, 'Full');

		const live = this.cache.getInstance(instance.id);
		if (live) {
      console.log("FOUNT CACHE")
			execution = live;
		}
		else {
      console.log("FOUNT LIVE")
			execution = await Execution.restore(this.server,instance,itemId);

			execution.isLocked = true;
			/* new dataStore for every execution to be monitored 
			const newDataStore = new DataStore(execution.server);
			execution.server.dataStore = newDataStore;

			newDataStore.monitorExecution(execution); */

			this.cache.add(execution);
			this.logger.log("restore completed: "+instance.saved);

		}

		return execution;
	}
	async invokeItem(itemQuery, data = {}): Promise<Execution> {

		return await this.invoke(itemQuery, data);
	}
	/**
	 * update an existing item that is in a wait state with an assignment
	 * can modify data or assignment or both
	 * 
	 * -------------------------------------------------
	 *		
	 * @param itemQuery		criteria to retrieve the item
	 * @param data
	 */
	async assign(itemQuery, data = {}, assignment = {}, userName: string,options= {}): Promise<Execution> {
		
		this.logger.log(`^Action:engine.assign`);
		this.logger.log(itemQuery);
		let execution;

		this.runningCounter++;
		this.callsCounter++;
		try {

			const items = await this.server.dataStore.findItems(itemQuery);
			if (items.length > 1) {
				this.logger.error(`query produced more than ${items.length} items expecting only one`+JSON.stringify(itemQuery));
			}
			const item = items[0];
			if (!item) {
				this.logger.error("query produced no items for "+JSON.stringify(itemQuery));
			}

			execution = await this.restore(item.instanceId);

			execution.worker = execution.assign(item.id, this.sanitizeData(data), assignment, userName,options);

			await this.release(execution);

			return execution;
		}
		catch (exc) {
			return await this.exception(exc,execution); 

		}
		finally {
			this.runningCounter--;
			if (execution && execution.isLocked)
				await this.release(execution);
		}

	}
	/**
     * Continue an existing item that is in a wait state
     *
     * -------------------------------------------------
     * 
     * scenario:
     * 
     * ```
     * itemId 	{itemId: value }
     * itemKey 	{itemKey: value}
     * instance,task	{instanceId: instanceId, elementId: value }
     * ```
	 *		
	 * @param itemQuery		criteria to retrieve the item
	 * @param data
	 */
	async invoke(itemQuery, data = {}, userName: string = null, options = {}): Promise<Execution> {

		this.logger.log(`^Action:engine.invoke`);
		this.logger.log(itemQuery);
		let execution;
		this.runningCounter++;
		this.callsCounter++;

		try {

			const items = await this.server.dataStore.findItems(itemQuery);
			if (items.length > 1) {
				this.logger.error(`query produced more than ${items.length} items expecting only one`+JSON.stringify(itemQuery));
			}
			const item = items[0];
			if (!item) {
				this.logger.error("query produced no items for "+JSON.stringify(itemQuery));
			}

			if (item.status !== 'wait') {
				this.logger.log(`*****Item status is not in wait state ${item.status} ${item.elementId}-${item.processName}`)
                    //this.logger.error(`Item status is not in wait state`);
            }
			execution = await this.restore(item.instanceId);

			execution.worker = execution.signalItem(item.id, this.sanitizeData(data),userName,options);

			try {
				if (options['noWait'] == true) {
					this.logger.log(`.noWait`);
					execution.worker.then(obj=>{ 
						this.logger.log('after worker is done releasing ..'+item.instanceId);
						this.release(execution);
						});
					return execution;
				}
				else {
					const waiter = await execution.worker;
					this.logger.log(`.engine.continue ended`);

					await this.release(execution);
					return execution;
				}
			}
			catch(exc)
			{
					await this.release(execution);
					throw exc;
			}

			finally {
				if (execution && execution.isLocked)
					await this.release(execution);
			}
			}
		catch (exc) {
			return await this.exception(exc,execution); 
		}
		finally {
			this.runningCounter--;
			if (execution && execution.isLocked)
				await this.release(execution);
		}
	}
	/**
	 * 
	 *	Repeat Timers need to create new Item
	 * @param instanceId
	 * @param elementId
	 * @param data
	 */
	async startRepeatTimerEvent(instanceId, prevItem, data = {},options={}) : Promise<Execution> {

		// need to load instance first
		this.logger.log('startRepeatTimeEvent');
		let execution;

		try {

			execution= await this.restore(instanceId);

			await execution.signalRepeatTimerEvent(instanceId,prevItem,this.sanitizeData(data),options);

			await this.release(execution);

			this.logger.log("StartRepeatTimerEvent completed "+execution.isLocked);

			return execution;
		}
		catch (exc) {
			return await this.exception(exc,execution); 

		}
		finally {
			if (execution && execution.isLocked)
				await this.release(execution);
		}
	}
	/**
	 * 
	 * Invoking an event (usually start event of a secondary process) against an existing instance
	 * or
	 * Invoking a start event (of a secondary process) against an existing instance
	 * ----------------------------------------------------------------------------
	 *	 instance,task 
	 *```
	 *	{instanceId: instanceId, elementId: value } 
	 *```
	 *		
	 * @param instanceId
	 * @param elementId
	 * @param data
	 */
	async startEvent(instanceId, elementId, data = {},userName: string = null, options = {}) : Promise<Execution> {

		// need to load instance first
		this.logger.log('serverinvokeSignal');
		let execution;

		try {

			execution= await this.restore(instanceId);

			await execution.signalEvent(elementId, this.sanitizeData(data),userName,options);

			await this.release(execution);

			this.logger.log("Engine.StartEvent completed "+execution.isLocked);

			return execution;
		}
		catch (exc) {
			return await this.exception(exc,execution); 

		}
		finally {
			if (execution && execution.isLocked)
				await this.release(execution);
		}

	}
	async throwMessage(messageId, data = {}, matchingQuery = {}): Promise<Execution> {

		this.logger.log('..^Action:engine.throwMessage ', messageId,this.sanitizeData(data),matchingQuery);

		if (!messageId)
			return null;
		// need to load instance first
		const eventsQuery = { "events.messageId": messageId };
		const events = await this.definitions.findEvents(eventsQuery);

		this.logger.log('..findEvents ' + events.length);
		if (events.length > 0) {

			const event = events[0];
			this.logger.log('..^Action:engine.throwMessage found target event ', event.modelName, JSON.stringify(data), event.elementId, event.elementId);
			let ret = await this.start(event.modelName, data, event.elementId, event.elementId);
			this.logger.log('..^Action:engine.throwMessage ended', event.modelName, JSON.stringify(data), event.elementId, event.elementId);
			return ret;
		}
		let itemsQuery = {};
		if (matchingQuery)
			itemsQuery = Object.assign({}, matchingQuery);

		itemsQuery["items.messageId"] = messageId;
		itemsQuery["items.status"] = 'wait';


		const items = await this.dataStore.findItems(itemsQuery);

		if (items.length > 0) {

			const item = items[0];
			this.logger.log(`Throw Signal ${messageId} found target: ${item.processName} ${item.id}`);

			this.logger.log('..^Action:engine.throwMessage found target ', item.processName, item.id);
			return await this.invoke({ "items.id": item.id }, this.sanitizeData(data));
		}
		else {
			this.logger.log('** engine.throwMessage failed to find a target for ',JSON.stringify(itemsQuery));
			console.log('** engine.throwMessage failed to find a target for ', JSON.stringify(itemsQuery));

        }
		return null;

	}
	/**
	 * 
	 * signal/message raise a signal or throw a message 
	 * 
	 * will seach for a matching event/task given the signalId/messageId
	 * 
	 * that can be againt a running instance or it may start a new instance 
	 * ----------------------------------------------------------------------------
	 * @param messageId		the id of the message or signal as per bpmn definition
	 * @param matchingQuery	should match the itemKey (if specified)
	 * @param data			message data
	 */
	async throwSignal(signalId, data = {}, matchingQuery = {} ) {

		this.logger.log('..^Action:engine.Throw Signal ',signalId,this.sanitizeData(data),matchingQuery);

		var instances = [];
		if (!signalId)
			return null;

		// need to load instance first
		const eventsQuery = { "events.signalId": signalId };
		const events = await this.definitions.findEvents(eventsQuery);
		this.logger.log('..findEvents '+events.length);
		if (events.length > 0) {
			for (var i = 0; i < events.length; i++) {
				let event = events[i];
				this.logger.log('..^Action:engine.Throw Signal found target', event.modelName, data, event.elementId);
				
				var res = await this.start(event.modelName, this.sanitizeData(data), event.elementId, null);
				this.logger.log('Signal end data',res.instance.data)
				instances.push(res.instance.id);
			}
    }
		let itemsQuery = {};
		if (matchingQuery)
			itemsQuery = Object.assign({}, matchingQuery);

		itemsQuery["items.signalId"] = signalId;
		itemsQuery["items.status"] = 'wait';

		const items = await this.dataStore.findItems(itemsQuery);
		console.log('throw signal itemsQuery:', itemsQuery,items.length);
		if (items.length > 0) {
			for (var i = 0; i < items.length; i++) {
				let item = items[i];
				console.log(`Throw Signal ${signalId} found target: ${item.processName} ${item.id}`);
			}

			for (var i = 0; i < items.length; i++) {
				let item = items[i];
//				console.log(`Throw Signal ${signalId} found target: ${item.processName} ${item.id}`);
				this.logger.log('..^Action:engine.Throw Signal found target', item.processName,item.id );
				var res=await this.invoke({ "items.id": item.id }, this.sanitizeData(data));
				instances.push(res.instance.id);
            }
		}
		return instances;
	}
	private async exception(exc,execution) {

		if (execution)
			await execution.doExecutionEvent(execution,EXECUTION_EVENT.process_exception);

		return this.logger.error(exc);

	}
	private sanitizeData(data) {
		return JSON.parse(JSON.stringify(data));
	}
}


class InstanceMigrator extends ServerComponent implements IMigrator {
  public constructor(server: IBPMNServer) {
    super(server);
  }

  public async migrate(sourceExecution: Execution, execution: Execution) {
    const oldTokens = Array.from(sourceExecution.tokens.values())

    console.log("Starting migration...")
    console.log(`Source has ${oldTokens.length} tokens`)

    await execution.definition.load();

    console.log(`Successfully loaded target definition`)
    const newTokens = await this.migrateTokens(oldTokens, sourceExecution, execution);

    console.log(newTokens.map((token) => token.path))
    console.log(`Target exectuion has ${newTokens.length} tokens`)
    // const loops = this.migrateLoops(prevState)

    newTokens.forEach((token) => execution.tokens.set(token.id, token));
  }

  private async migrateTokens(tokens: Token[], sourceExecution: Execution, targetExecution: Execution){
    const migratedTokens: Token[] = [];

    const findParentToken = async (parentId: string) => {
      const foundParent = migratedTokens.find((token) => token.id === parentId);
      if (foundParent) {
        return foundParent;
      }

      const unprocessedParent = tokens.find((token) => token.id === parentId);
      if (!unprocessedParent) {
        throw new Error("Could not find parent token with id: " + parentId);
      }

      const token = await this.processTokenMigration(unprocessedParent, sourceExecution, targetExecution, findParentToken)
      migratedTokens.push(token);
      return token;
    }

    for (const token of tokens) {
      migratedTokens.push(await this.processTokenMigration(token, sourceExecution, targetExecution, findParentToken))
    }

    console.log(`migratedTokens: ${migratedTokens.length}`)

    return migratedTokens;
  }

  private async processTokenMigration(token: Token, sourceExecution: Execution, targetExecution: Execution, findParentCallback: (parentId: string) => Promise<Token | undefined>): Promise<Token> {
    let migratedParentToken;
    if (token.parentToken) {
      migratedParentToken = await findParentCallback(token.parentToken.id)

      if (!migratedParentToken) {
        const error = "***Error*** Node migration failed. Parent not found"
        this.logger.log(error)
        throw new Error(error)
      }
    }

    const migratedToken = await this.migrateToken(token, targetExecution, migratedParentToken)
    const sourceItem = sourceExecution.getItems().find((item) => item.elementId === token.currentNode.id && item.tokenId === token.id);
    if (!sourceItem) {
      const error = "***Error*** Node migration failed. Migrated token item not found"
      this.logger.log(error)
      throw new Error(error)
    }

    // TODO: We need to change the queries the engine runs. This is needed because some query searches all items in all instances, expecting all ids to be different.
    const clonedItem = {...sourceItem} as Item
    clonedItem.id = uuidv4()

    migratedToken.path = [clonedItem]

    return migratedToken;
  }

  // TODO: Add migration of input and output fields
  // TODO: Add possibility to map the tokens to new nodes if theirs do not exist (creat a new Item then, and remove the current. Use "goNext")
  private async migrateToken(token: Token, targetExecution: Execution, parent: Token): Promise<Token> {
    const tokenCurrentNode = targetExecution.definition.getNodeById(token.currentNode?.id);
    const tokenStartNode = targetExecution.definition.getNodeById(token.startNodeId);

    if (!tokenCurrentNode || !tokenStartNode) {
      console.log(`currentNodeId: ${tokenCurrentNode?.id} startNodeId: ${token.startNodeId}`)
      console.log(`Expected`)
      console.log(`currentNodeId: ${token.currentNode?.id} startNodeId: ${token.currentNode?.id}`)
      // TODO: Migrate with mapping in this case
      const error = "***Error*** Node migration failed"
      this.logger.log(error)
      throw new Error(error)
    }

    const migratedToken = await Token.startNewToken(token.type, targetExecution, token.startNodeId, token.dataPath, parent, null, null, token.data, true, token.itemsKey)
    // const migratedToken = new Token(token.type, targetExecution, tokenStartNode, token.dataPath, parent)
    migratedToken.id = token.id;
    migratedToken.startNodeId = token.startNodeId;
    migratedToken.currentNode = tokenCurrentNode;
    migratedToken.status = token.status;
    migratedToken.itemsKey = token.itemsKey;
    migratedToken.path = [];

    console.log(`Migrated token with node ${tokenCurrentNode.id} and start node ${tokenStartNode.id}`)

    return migratedToken;
  }

  // TODO: Loop migration should have settings to restart looping or move it
  private migrateLoops(prevState: IInstanceData): Loop[] {
    if (!prevState.loops?.length) {
      this.logger.log("***Error*** Loop migration is not supported yet.")
    }

    return [];
  }

}

export { Engine, InstanceMigrator };
