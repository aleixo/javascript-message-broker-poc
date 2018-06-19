import React, { Component } from 'react';
import uuidv4 from 'uuid/v4';
import logo from './logo.svg';
import './App.css';
import PrimusChat from './lib/primus_CHAT';
import PrimusTickets from './lib/primus_TICKETS';
import PrimusVoice from './lib/primus_VOICE';
import { Checkbox, Form, FormControl,ControlLabel,FormGroup,FieldGroup,Button, ButtonToolbar } from 'react-bootstrap/lib';

import SocketMessage from './models/SocketMessage';

const VOICE_PORT = 4001;
const CHAT_PORT = 4002;
const TICKETS_PORT = 4003;

const VOICE_CHANNEL = 'VOICE';
const CHAT_CHANNEL = 'CHAT';
const TICKETS_CHANNEL = 'TICKETS';

const VOICE_BACKUP_PORT = 5001;
const CHAT_BACKUP_PORT = 5002;
const TICKETS_BACKUP_PORT = 5003;

const voiceUserUuid = uuidv4();
const chatUserUuid = uuidv4();
const ticketsUserUuid = uuidv4();

let primusTickets;
let primusVoice;
let primusChat

class App extends Component {
  constructor() {
    super(); 
    this.prevSocketId = {
      voice : '',
      chat: '',
      tickets: '',
    };

    this.state = {
      sockets : {
        hasVoice : false,
        hasChat : false,
        hasTickets : false
      },
      ACTION: '',
      MESSAGE: '',
      REPLY : false
    };

    this.voiceInterval;
  }
  
  connectVoice(port) {
    console.log('WILL TRY TO CONNECT TO',port)
    if (this.state.sockets.hasVoice) {
      primusVoice.end();
      return this.setState({
        sockets : {
          hasVoice : false,
          hasTickets : this.state.sockets.hasTickets,
          hasChat : this.state.sockets.hasChat,
        }});
    }
    primusVoice = new PrimusVoice(`http://localhost:${port}`, { reconnect: { min: 500 , retries: 1 } });              
    primusVoice.on('open', () => {         
      this.setState({
        sockets : {
          hasVoice : true,
          hasTickets : this.state.sockets.hasTickets,
          hasChat : this.state.sockets.hasChat,
        }}); 
      primusVoice.writeAndWait(new SocketMessage(true, 'VOICE MESSAGE', 'ACTION', VOICE_CHANNEL, false, 0, voiceUserUuid).data);        
    });          
    primusVoice.on('data', (data) => {   
      console.log('[VOICE SOCKET] ',data)   
      this.prevSocketId.voice = data.id       
    });
    primusVoice.on('end', (data) => { console.log('CON END') });
    primusVoice.on('reconnect failed', (err, opts) => {
      this.setState({
        sockets : {
          hasVoice : false,
          hasTickets : this.state.sockets.hasTickets,
          hasChat : this.state.sockets.hasChat,
        }}, () => {
          setTimeout( () => {
            console.log('WILL TRY TO CONNECT TO OTHER PORT');
            this.connectVoice(port === VOICE_BACKUP_PORT ? VOICE_PORT : VOICE_BACKUP_PORT)
          }, 2000)       
        });
    });
    primusVoice.on('request', function(data, done) {
      console.log('ON REQUEST', data);
      done('this is the response');
  });
  }

  connectChat(port) {
    if (this.state.sockets.hasChat) {
      primusChat.end();
      return this.setState({
        sockets : {
          hasVoice : this.state.sockets.hasVoice,
          hasTickets : this.state.sockets.hasTickets,
          hasChat : false,
        }});
    }
    primusChat = new PrimusChat(`http://localhost:${port}`, { reconnect: { min: 500 , retries: 1 } }); 
    primusChat.on('open', () => {      
      this.setState({
        sockets : {
          hasVoice : this.state.sockets.hasVoice,
          hasTickets : this.state.sockets.hasTickets,
          hasChat : true,
        }});
      primusChat.writeAndWait(new SocketMessage(true, 'CHAT MESSAGE', 'ACTION', CHAT_CHANNEL, false, 0, 1, uuidv4()).data);
    });      
    primusChat.on('data', (data) => {
      console.log('[CHAT SOCKET] ',data)  
      this.prevSocketId.chat = data.id    
    });
    primusChat.on('end', (data) => { console.log('CON END') }); 
    primusChat.on('reconnect failed', (err, opts) => {
      this.setState({
        sockets : {
          hasVoice : this.state.sockets.hasVoice,
          hasTickets : this.state.sockets.hasTickets,
          hasChat : false,
        }}, () => {
          setTimeout( () => {
            console.log('WILL TRY TO CONNECT TO OTHER PORT');
            this.connectChat(port === CHAT_BACKUP_PORT ? CHAT_PORT : CHAT_BACKUP_PORT)
          }, 2000)       
        });
    });
    primusChat.on('request', function(data, done) {
      console.log('ON REQUEST', data);
      done('this is the response');
  });
  }

  connectTickets(port) {
    if (this.state.sockets.hasTickets) {
      primusTickets.destroy();
      return this.setState({
        sockets : {
          hasVoice : this.state.sockets.hasVoice,
          hasTickets : false,
          hasChat : this.state.sockets.hasChat,
        }});
    }
    primusTickets = new PrimusTickets(`http://localhost:${port}`, { reconnect: { min: 500 , retries: 1 } });                   
    primusTickets.on('open', () => {      
      this.setState({
        sockets : {
          hasVoice : this.state.sockets.hasVoice,
          hasTickets : true,
          hasChat : this.state.sockets.hasChat,
        }});
      primusTickets.write(new SocketMessage(true, 'TICKETS MESSAGE', 'ACTION', TICKETS_CHANNEL, false, 0, 1, uuidv4()).data);
    });      
    primusTickets.on('data', (data) => {   
      console.log('[TICKETS SOCKET] ',data)     
      this.prevSocketId.tickets = data.id       
    });  
    primusTickets.on('end', (data) => { console.log('CON END') });
    primusTickets.on('reconnect failed', (err, opts) => {
      this.setState({
        sockets : {
          hasVoice : this.state.sockets.hasVoice,
          hasTickets : false,
          hasChat : this.state.sockets.hasChat,
        }}, () => {
          setTimeout( () => {
            console.log('WILL TRY TO CONNECT TO OTHER PORT');
            this.connectTickets(port === TICKETS_BACKUP_PORT ? TICKETS_PORT : TICKETS_BACKUP_PORT)
          }, 2000)       
        });
    });  
    primusTickets.on('request', function(data, done) {
      console.log('ON REQUEST', data);
      done('this is the response');
  });
  }

  handleFormChange(e) {    
    this.setState({
      [e.target.name] : e.target.hasOwnProperty('checked') ? e.target.checked : e.target.value,
    }, () => console.log(this.state));    
  }

  handleFormSubmit(e) {        
    e.preventDefault();    
    const mapper = {
      'VOICE' : (message, reply, workTime) => this.state.sockets.hasVoice && primusVoice.write(new SocketMessage(false, message, 'DO VOICE THINGS', VOICE_CHANNEL, !reply, workTime).data),
      'CHAT' : (message, reply, workTime) => this.state.sockets.hasChat &&  primusChat.write(new SocketMessage(false, message, 'DO CHAT THINGS', CHAT_CHANNEL, !reply, workTime).data),
      'TICKETS' : (message, reply, workTime) => this.state.sockets.hasTickets &&  primusTickets.write(new SocketMessage(false, message, 'DO TICKETS THINGS', TICKETS_CHANNEL, !reply, workTime).data)
    }    
    mapper[this.state.ACTION].call(this,this.state.MESSAGE,this.state.REPLY,this.state.WORK_TIME)
  }

  render() {
    return (
      <div className="App">        
        <head>
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css" integrity="sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u" crossOrigin="anonymous" />
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap-theme.min.css" integrity="sha384-rHyoN1iRsVXV4nD0JutlnGaslCJuC7uwjduW9SVrLvRYooPp2bWYgmgJQIXwl/Sp" crossOrigin="anonymous" />
        </head>
        <header className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <h1 className="App-title">POC RabbitMq</h1>
        </header>
        <div style={{paddingTop: '50px',display: 'flex', justifyContent: 'center'}}>
        <ButtonToolbar>
          <Button              
              bsStyle={this.state.sockets.hasVoice ? "success" : "warning"}
              onClick={this.connectVoice.bind(this, VOICE_PORT)}              
          > VOICE
          </Button>
          <Button              
              bsStyle={this.state.sockets.hasChat ? "success" : "warning"}
              onClick={this.connectChat.bind(this, CHAT_PORT)}              
          > CHAT
          </Button>
          <Button              
              bsStyle={this.state.sockets.hasTickets ? "success" : "warning"}
              onClick={this.connectTickets.bind(this, TICKETS_PORT)}              
          > TICKETS
          </Button>
        </ButtonToolbar>  
        </div>   
        <div style={{display: 'flex', justifyContent: 'center'}}>
        <Form horizontal onSubmit={this.handleFormSubmit.bind(this)}>                       
          <FormGroup onChange={this.handleFormChange.bind(this)} >
            
            <ControlLabel>Select</ControlLabel>
            <FormControl name="ACTION" componentClass="select" placeholder="Action">
              <option value="SELECT">select</option>
              <option value="VOICE">VOICE</option>
              <option value="CHAT">CHAT</option>
              <option value="TICKETS">TICKETS</option>
            </FormControl>

            <ControlLabel>Message</ControlLabel>
            <FormControl            
              name="MESSAGE"
              type="text"
              label="Message"
              placeholder="Enter message"
            />
            <ControlLabel>Work time</ControlLabel>
            <FormControl name="WORK_TIME" componentClass="select" placeholder="Action">
              <option value="0">0</option>
              <option value="1">1</option>
              <option value="2">2</option>
              <option value="3">3</option>
              <option value="4">4</option>
              <option value="5">5</option>
              <option value="6">6</option>
              <option value="7">7</option>
              <option value="8">8</option>
              <option value="9">9</option>
              <option value="10">10</option>
              <option value="15">15</option>
              <option value="20">20</option>
              <option value="30">30</option>
            </FormControl> 
            <ControlLabel>Reply</ControlLabel>
            <Checkbox                
                name="REPLY"
                value={this.state.REPLY}                
            />
          </FormGroup>
                      
          <Button type="submit">Submit</Button>
        </Form>
        </div>            
      </div>
    );
  }
}

export default App;
