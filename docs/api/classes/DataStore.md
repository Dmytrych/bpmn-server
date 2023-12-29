[bpmn-server](../API.md) / DataStore

# Class: DataStore

super class for various objects that are part of the server

## Hierarchy

- [`ServerComponent`](ServerComponent.md)

  ↳ **`DataStore`**

## Implements

- [`IDataStore`](../interfaces/IDataStore.md)

## Table of contents

### Constructors

- [constructor](DataStore.md#constructor)

### Properties

- [dbConfiguration](DataStore.md#dbconfiguration)
- [db](DataStore.md#db)
- [execution](DataStore.md#execution)
- [isModified](DataStore.md#ismodified)
- [isRunning](DataStore.md#isrunning)
- [inSaving](DataStore.md#insaving)
- [promises](DataStore.md#promises)
- [locker](DataStore.md#locker)
- [seq](DataStore.md#seq)
- [server](DataStore.md#server)

### Accessors

- [configuration](DataStore.md#configuration)
- [logger](DataStore.md#logger)
- [cron](DataStore.md#cron)
- [cache](DataStore.md#cache)
- [appDelegate](DataStore.md#appdelegate)
- [engine](DataStore.md#engine)
- [dataStore](DataStore.md#datastore)
- [definitions](DataStore.md#definitions)
- [listener](DataStore.md#listener)

### Methods

- [save](DataStore.md#save)
- [loadInstance](DataStore.md#loadinstance)
- [getItemsFromInstances](DataStore.md#getitemsfrominstances)
- [saveInstance](DataStore.md#saveinstance)
- [findItem](DataStore.md#finditem)
- [findInstance](DataStore.md#findinstance)
- [convertObj](DataStore.md#convertobj)
- [convertColl](DataStore.md#convertcoll)
- [findInstances](DataStore.md#findinstances)
- [findItems](DataStore.md#finditems)
- [deleteInstances](DataStore.md#deleteinstances)
- [install](DataStore.md#install)

## Constructors

### constructor

• **new DataStore**(`server`): [`DataStore`](DataStore.md)

#### Parameters

| Name | Type |
| :------ | :------ |
| `server` | [`IBPMNServer`](../interfaces/IBPMNServer.md) |

#### Returns

[`DataStore`](DataStore.md)

#### Overrides

[ServerComponent](ServerComponent.md).[constructor](ServerComponent.md#constructor)

#### Defined in

[datastore/DataStore.ts:33](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L33)

## Properties

### dbConfiguration

• **dbConfiguration**: `any`

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[dbConfiguration](../interfaces/IDataStore.md#dbconfiguration)

#### Defined in

[datastore/DataStore.ts:23](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L23)

___

### db

• **db**: `any`

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[db](../interfaces/IDataStore.md#db)

#### Defined in

[datastore/DataStore.ts:24](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L24)

___

### execution

• **execution**: [`Execution`](Execution.md)

#### Defined in

[datastore/DataStore.ts:26](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L26)

___

### isModified

• **isModified**: `boolean` = `false`

#### Defined in

[datastore/DataStore.ts:27](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L27)

___

### isRunning

• **isRunning**: `boolean` = `false`

#### Defined in

[datastore/DataStore.ts:28](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L28)

___

### inSaving

• **inSaving**: `boolean` = `false`

#### Defined in

[datastore/DataStore.ts:29](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L29)

___

### promises

• **promises**: `any`[] = `[]`

#### Defined in

[datastore/DataStore.ts:30](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L30)

___

### locker

• **locker**: `any`

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[locker](../interfaces/IDataStore.md#locker)

#### Defined in

[datastore/DataStore.ts:31](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L31)

___

### seq

▪ `Static` **seq**: `number` = `0`

#### Defined in

[datastore/DataStore.ts:89](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L89)

___

### server

• **server**: `any`

#### Inherited from

[ServerComponent](ServerComponent.md).[server](ServerComponent.md#server)

#### Defined in

[server/ServerComponent.ts:9](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L9)

## Accessors

### configuration

• `get` **configuration**(): `any`

#### Returns

`any`

#### Inherited from

ServerComponent.configuration

#### Defined in

[server/ServerComponent.ts:14](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L14)

___

### logger

• `get` **logger**(): `any`

#### Returns

`any`

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[logger](../interfaces/IDataStore.md#logger)

#### Inherited from

ServerComponent.logger

#### Defined in

[server/ServerComponent.ts:15](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L15)

___

### cron

• `get` **cron**(): [`Cron`](Cron.md)

#### Returns

[`Cron`](Cron.md)

#### Inherited from

ServerComponent.cron

#### Defined in

[server/ServerComponent.ts:16](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L16)

___

### cache

• `get` **cache**(): [`CacheManager`](CacheManager.md)

#### Returns

[`CacheManager`](CacheManager.md)

#### Inherited from

ServerComponent.cache

#### Defined in

[server/ServerComponent.ts:17](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L17)

___

### appDelegate

• `get` **appDelegate**(): `any`

#### Returns

`any`

#### Inherited from

ServerComponent.appDelegate

#### Defined in

[server/ServerComponent.ts:18](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L18)

___

### engine

• `get` **engine**(): [`IEngine`](../interfaces/IEngine.md)

#### Returns

[`IEngine`](../interfaces/IEngine.md)

#### Inherited from

ServerComponent.engine

#### Defined in

[server/ServerComponent.ts:19](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L19)

___

### dataStore

• `get` **dataStore**(): `any`

#### Returns

`any`

#### Inherited from

ServerComponent.dataStore

#### Defined in

[server/ServerComponent.ts:20](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L20)

___

### definitions

• `get` **definitions**(): `any`

#### Returns

`any`

#### Inherited from

ServerComponent.definitions

#### Defined in

[server/ServerComponent.ts:21](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L21)

___

### listener

• `get` **listener**(): `any`

#### Returns

`any`

#### Inherited from

ServerComponent.listener

#### Defined in

[server/ServerComponent.ts:22](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/server/ServerComponent.ts#L22)

## Methods

### save

▸ **save**(`instance`, `options?`): `Promise`\<`void`\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `instance` | `any` |
| `options` | `Object` |

#### Returns

`Promise`\<`void`\>

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[save](../interfaces/IDataStore.md#save)

#### Defined in

[datastore/DataStore.ts:46](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L46)

___

### loadInstance

▸ **loadInstance**(`instanceId`, `options?`): `Promise`\<\{ `instance`: [`IInstanceData`](../interfaces/IInstanceData.md) = instanceData; `items`: `any`[]  }\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `instanceId` | `any` |
| `options` | `Object` |

#### Returns

`Promise`\<\{ `instance`: [`IInstanceData`](../interfaces/IInstanceData.md) = instanceData; `items`: `any`[]  }\>

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[loadInstance](../interfaces/IDataStore.md#loadinstance)

#### Defined in

[datastore/DataStore.ts:49](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L49)

___

### getItemsFromInstances

▸ **getItemsFromInstances**(`instances`, `condition?`, `trans?`): `any`[]

#### Parameters

| Name | Type | Default value |
| :------ | :------ | :------ |
| `instances` | `any` | `undefined` |
| `condition` | `any` | `null` |
| `trans` | `any` | `null` |

#### Returns

`any`[]

#### Defined in

[datastore/DataStore.ts:67](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L67)

___

### saveInstance

▸ **saveInstance**(`instance`, `options?`): `Promise`\<`void`\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `instance` | `any` |
| `options` | `Object` |

#### Returns

`Promise`\<`void`\>

#### Defined in

[datastore/DataStore.ts:90](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L90)

___

### findItem

▸ **findItem**(`query`): `Promise`\<[`IItemData`](../interfaces/IItemData.md)\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `query` | `any` |

#### Returns

`Promise`\<[`IItemData`](../interfaces/IItemData.md)\>

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[findItem](../interfaces/IDataStore.md#finditem)

#### Defined in

[datastore/DataStore.ts:132](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L132)

___

### findInstance

▸ **findInstance**(`query`, `options`): `Promise`\<[`IInstanceData`](../interfaces/IInstanceData.md)\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `query` | `any` |
| `options` | `any` |

#### Returns

`Promise`\<[`IInstanceData`](../interfaces/IInstanceData.md)\>

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[findInstance](../interfaces/IDataStore.md#findinstance)

#### Defined in

[datastore/DataStore.ts:142](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L142)

___

### convertObj

▸ **convertObj**(`obj`, `cls`): `any`

#### Parameters

| Name | Type |
| :------ | :------ |
| `obj` | `any` |
| `cls` | `any` |

#### Returns

`any`

#### Defined in

[datastore/DataStore.ts:163](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L163)

___

### convertColl

▸ **convertColl**(`coll`, `cls`): `void`

#### Parameters

| Name | Type |
| :------ | :------ |
| `coll` | `any` |
| `cls` | `any` |

#### Returns

`void`

#### Defined in

[datastore/DataStore.ts:167](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L167)

___

### findInstances

▸ **findInstances**(`query`, `option?`): `Promise`\<[`IInstanceData`](../interfaces/IInstanceData.md)[]\>

#### Parameters

| Name | Type | Default value |
| :------ | :------ | :------ |
| `query` | `any` | `undefined` |
| `option` | `any` | `'summary'` |

#### Returns

`Promise`\<[`IInstanceData`](../interfaces/IInstanceData.md)[]\>

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[findInstances](../interfaces/IDataStore.md#findinstances)

#### Defined in

[datastore/DataStore.ts:177](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L177)

___

### findItems

▸ **findItems**(`query`): `Promise`\<[`IItemData`](../interfaces/IItemData.md)[]\>

scenario:

```
itemId			{ items { id : value } }
itemKey			{ items {key: value } }
instance, task	{ instance: { id: instanceId }, items: { elementId: value }}
message			{ items: { messageId: nameofmessage, key: value } {}
status			{ items: {status: 'wait' } }
custom: { query: query, projection: projection }
```
New approach:
just like MongoDB
```
itemId			{ items { id : value } }
itemKey			{ items {key: value } }
instance, task	{  id: instanceId , items.elementId: value }
message			{ items.messageId: nameofmessage, key: value } {}
status			{ items.status: 'wait' } }
custom: { query: query, projection: projection }
```

Problem with Mongodb:	`projection $elematch returns only the first record`

#### Parameters

| Name | Type |
| :------ | :------ |
| `query` | `any` |

#### Returns

`Promise`\<[`IItemData`](../interfaces/IItemData.md)[]\>

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[findItems](../interfaces/IDataStore.md#finditems)

#### Defined in

[datastore/DataStore.ts:215](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L215)

___

### deleteInstances

▸ **deleteInstances**(`query`): `Promise`\<`any`\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `query` | `any` |

#### Returns

`Promise`\<`any`\>

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[deleteInstances](../interfaces/IDataStore.md#deleteinstances)

#### Defined in

[datastore/DataStore.ts:229](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L229)

___

### install

▸ **install**(): `Promise`\<`void`\>

first time installation of DB

creates a new collection and add an index

#### Returns

`Promise`\<`void`\>

#### Implementation of

[IDataStore](../interfaces/IDataStore.md).[install](../interfaces/IDataStore.md#install)

#### Defined in

[datastore/DataStore.ts:242](https://github.com/bpmnServer/bpmn-server/blob/637b6d1/src/datastore/DataStore.ts#L242)