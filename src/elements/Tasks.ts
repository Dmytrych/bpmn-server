
import { Node } from './Node';

import { Token, TOKEN_TYPE } from '../engine/Token';
import { BPMN_TYPE, ITEM_STATUS, NODE_ACTION } from '../interfaces/Enums'

import { Process } from './Process';
import { IExecution } from '../interfaces/engine';
import { EXECUTION_STATUS } from '../interfaces/Enums';
import { Item } from '../engine/Item';
import { ScriptHandler } from '../';
//NO_import { DecisionTable } from 'dmn-engine';

// ---------------------------------------------
class ScriptTask extends Node {
    async run(item): Promise<NODE_ACTION> {

        if (this.def.script) {
            item.token.log('executing script task');
            item.token.log(this.def.script);
            await ScriptHandler.executeScript(item, this.def.script);
        }
        return NODE_ACTION.end;
    }
}
/**
 * ```ts
 *      foo[method]();
 *      
 *      await this.token.appDelegate[serviceName](data);
 *      
 *      
 *      service signature:
 *              output= service (input,context)
 * ```
 */
class ServiceTask extends Node {
    get serviceName() {
        if (this.def.implementation && typeof this.def.implementation!=='undefined') {
            return this.def.implementation;
        }
        else if (this.def.delegateExpression) {
            return this.def.delegateExpression;
        }

    }
    async run(item): Promise<NODE_ACTION> {

        item.context.action = null;
        item.context.item = item;
        // calling appDelegate by service name
        const appDelegate = item.token.execution.appDelegate;

        // let output = await item.node.getOutput(item);

        let ret;

        item.log("invoking service:" + this.serviceName + " input:" + JSON.stringify(item.input));

        const servicesProvider=await appDelegate.getServicesProvider(item.token.execution);
//
        let obj = servicesProvider;

      let method = this.serviceName;
        if (obj && this.serviceName)  {

            const objs = this.serviceName.split('.');
            for (var i = 0; i < objs.length - 1; i++) {
                const o = objs[i];
                obj = obj[o];
            }
            method = objs[objs.length - 1];
        }

        if (obj && obj[method]) {
            ret = await obj[method](item.input, item.context);
        }
        else {
            ret = await appDelegate['serviceCalled'](item.input, item.context);
        }
        
        item.log("service returned " + ret);
        item.output = ret;
        item.log('service '+ this.serviceName+' completed-output:' + ret + item.output);
        // await item.node.setInput(item,ret);

        if (item.context.action && item.context.action == NODE_ACTION.wait) {

            return item.context.action;
        }

        return NODE_ACTION.end;
    }
    describe() {

        let desc = super.describe();
        desc.push([[`service delegate:`,` ${this.serviceName}`]]);
        return desc;
    }
}
//    <bpmn2:businessRuleTask id="Task_1lcamp6" name="Vacation"  camunda:decisionRef="Vacation">


class BusinessRuleTask extends ServiceTask {
    async run(item): Promise<NODE_ACTION> {
        let businessRule;
        const token: Token = item.token;

        const config= token.execution.server.configuration;
        const path = config.definitionsPath;

        //console.log('Business Rule Task'); //.loopCharacteristics.$attrs["camunda:collection"];
        if (this.def.$attrs && this.def.$attrs["camunda:decisionRef"]) {

            throw new Error("Business Rule Task Not supported in this release.");
            /*
            businessRule = this.def.$attrs["camunda:decisionRef"];
            console.log("invoking business rule:" + businessRule)
            const dt = await DecisionTable.load(path + businessRule + '.json');
            console.log(dt);
            const data = await item.node.getOutput(item);
            const result = await dt.evaluate(data);
            console.log("result");
            console.log(result.actions);
            
            await item.node.setInput(item, result.actions);
            */
        }
        return NODE_ACTION.end;
    }

}
class SendTask extends ServiceTask {

    get isCatching(): boolean { return false; } 
    /*
    async run(item): Promise<NODE_ACTION> {

        if (this.def.script) {
            item.token.log(this.def.script);
            await item.token.execution.appDelegate.scopeJS(item, this.def.script);
        }
        return NODE_ACTION.end;
    } */
}

class UserTask extends Node {

    async end(item: Item) {
        item.token.info(`Task ${this.name} ended by ${item.token.execution.userName}`);
        return await super.end(item);
    }
    async start(item: Item): Promise<NODE_ACTION> {
        if (this.def.$attrs)
            {
            await this.setAssignVal(item,"assignee");
            await this.setAssignVal(item,"candidateGroups");
            await this.setAssignVal(item,"candidateUsers");
            await this.setAssignVal(item,"dueDate",true);
            await this.setAssignVal(item,"followUpDate",true);
            await this.setAssignVal(item,"priority");
            }
        if (this.lane && this.lane !=='')
            {
            if (item.candidateGroups==null)
                item.candidateGroups=new Array();

            item.candidateGroups.push(this.lane);
            }

        item.token.info(`Task ${this.name} -${this.id} started.`);
        return await super.start(item);
    }
    async setAssignVal(item,attr,dateFormat=false) {
        const exp=this.def.$attrs["camunda:"+attr];

        if (!exp)
            return;
        var val;
        if (exp.startsWith('$'))
        {
            val = ScriptHandler.evaluateExpression(item, exp);
        }
        else if (exp.startsWith('#')) {
            val = await ScriptHandler.executeScript(item, exp);
        }
        else if (exp.includes(","))
        {
            const arr=exp.split(",");
            val=arr;
        }
        else
            val=exp;

        if (dateFormat)
            val=new Date(val);

        item[attr] = val;
        item.token.log('..setting attribute '+attr+' exp: '+exp+'='+val);
        //console.log('----setAttVal', attr, exp, val);
    }
    describe() {
        let attrs = ["assignee","candidateGroups", "candidateUsers", "dueDate", "followUpDate", "priority"];
        let desc = super.describe();

        attrs.forEach(attr => {
            let val = this.def.$attrs["camunda:" + attr];
            if (val)
                desc.push([attr, val]);
        });
        return desc;
    }
    get requiresWait() { return true; }
    get canBeInvoked() { return true; }
}
class ReceiveTask extends Node {

    get isCatching(): boolean { return true; } 
    get requiresWait() { return true; }
    get canBeInvoked() { return true; }
}

class SubProcess extends Node {
    childProcess: Process;
    get requiresWait() { return true; }
    get canBeInvoked() { return false; }

    async start(item): Promise<NODE_ACTION> {

        const token = item.token;

        token.log('..executing a sub process item:' + item.id );
        const startNode = this.childProcess.getStartNodes()[0];

        item.status = ITEM_STATUS.wait;

        const newToken = await Token.startNewToken(TOKEN_TYPE.SubProcess, token.execution,
                startNode, this.id, token, item, null, null, true);

        await this.childProcess.start(token.execution,newToken);

        await this.startBoundaryEvents(item, newToken);
        await newToken.execute(null);

        if (item.status == ITEM_STATUS.wait)
            return NODE_ACTION.wait;
        else
            return NODE_ACTION.continue;
    }
}
/**
 * 
 * <callActivity id="callCheckCreditProcess" name="Check credit" calledElement="checkCreditProcess" />
 * 
 * the called process need to call me back when done.
 * 
 *  Scenario
 *      a process 'D1' element 'E' is a 'CallActivity' calling Definition 'D2'
 * steps:
 * 
 *      1.  Item 'E': item start: start event is fired to prepare for input
 *      2.  Item 'E': item start: executing the call process passing input
 *      3.  If Process 'D2' is completed immediatly
 *              we continue, skip next steps
  *     4.  Process 'D1' goes on a wait state
 *      5.  When the process 'D2' is completed it calls 'executionEnded'
 *      6.  We continue with Item:Invoke 
 *      7.  event 'end' is fired to handle output of the process
 *      
 *      
 * 
 * */
class CallActivity extends Node {
    get calledElement() {
        return this.def.calledElement;
    };

    get requiresWait() { return true; }
    get canBeInvoked() { return false; }

    static async executionEnded(execution: IExecution) {
        const itemId = execution.instance.parentItemId;
        const engine = execution.server.engine;
        await engine.invoke({ "items.id": itemId }, execution.instance.data);

    }
    async start(item): Promise<NODE_ACTION> {

        const token = item.token;

        token.log('..executing a call activity for item:' + item.id+ " calling "+this.calledElement);

        const context = item.context;
        const modelName = this.calledElement;
        //const data = await item.node.getOutput(item);

        const response = await context.engine.start(modelName, item.input);

        token.log('..end of executing a call activity for item:' + item.id + " calling " + this.calledElement);

        token.log('..response :' + response.execution.status);

        if (response.execution.status == EXECUTION_STATUS.end)
            return NODE_ACTION.continue;
        else
            return NODE_ACTION.wait;
    }
}


export {  UserTask, ScriptTask, ServiceTask, BusinessRuleTask, SendTask, ReceiveTask, SubProcess , CallActivity }