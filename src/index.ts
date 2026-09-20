export class FlowWindow{constructor(public value=65535){}consume(size:number){if(size<0||size>this.value)throw new Error('flow control');this.value-=size}update(delta:number){const next=this.value+delta;if(delta<=0||next>0x7fffffff)throw new Error('invalid update');this.value=next}}
export type StreamState='idle'|'open'|'half-closed'|'closed';
export class Stream{state:StreamState='idle';open(){if(this.state!=='idle')throw new Error('state');this.state='open'}close(){this.state='closed'}}
