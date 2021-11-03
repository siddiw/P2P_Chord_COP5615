open System
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.Collections.Generic
open ChordNode
open System.Security.Cryptography
open System.Text

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : ERROR
            loglevel : ERROR
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

let chordSystem = ActorSystem.Create("ChordSystem", configuration)

type MainCommands =
    | StartAlgorithm of (int*int)
    | Create of (int*IActorRef)
    | Notify of (int*IActorRef)
    | Stabilize
    | FindNewNodeSuccessor of (int*IActorRef)
    | FoundNewNodeSuccessor of (int*IActorRef)
    | PredecessorRequest
    | PredecessorResponse of (int*IActorRef)
    | KeyLookup of (int*int*int)
    | FixFingers
    | FindithSuccessor of (int*int*IActorRef)
    | FoundFingerEntry of (int*int*IActorRef)
    | StartLookups of (int)
    | FoundKey of (int)
    

let mutable mainActorRef = null

let mutable numNodes = 0
let mutable numRequests = 0
let mutable m = 16
let mutable firstNodeId = 0
let mutable firstNodeRef = null
let mutable secondNodeRef = null
let StabilizeCycletimeMs = 100.0
let FixFingersCycletimeMs = 300.0

let mutable hashSpace = pown 2 m

let updateElement index element list = 
  list |> List.mapi (fun i v -> if i = index then element else v)

type FingerTableEntry(x:int, y:IActorRef) as this =
    let id = x
    let idRef = y
    member this.GetId() = x
    member this.GetRef() = y

let GetHashNumber data =
    use sha1Hash = SHA1Managed.Create()
    let mutable hash = sha1Hash.ComputeHash(Encoding.UTF8.GetBytes(data:string):byte[]) |> bigint
    if hash.Sign = -1 then
        hash <- bigint.Negate(hash)
    hash

let consistentHashFunc (m:double) (nodeNum:int) = 
    let name = string(nodeNum)
    let hash = GetHashNumber name 
    let x = m |> bigint
    let hashKey = (hash) % x  // identifier circle modulo 2^m
    let nodeName: string = hashKey |> string // assign a m- bit identifier using SHA-1 as a base hash function.
    nodeName


// Tracks and prints average hop count 
let PrinterActor (mailbox: Actor<_>) =
    let mutable hopCountSum = 0
    let mutable requestsCount = 0
    let endCondition = numNodes * numRequests

    let rec loop () =
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | FoundKey (hopCount) ->
                hopCountSum <- hopCountSum + hopCount
                requestsCount <- requestsCount + 1
                printfn "\n %d FoundKey = %d" requestsCount hopCount

                if requestsCount = endCondition then 
                    let avgHopCount = float(hopCountSum)/float(requestsCount)
                    printfn "\n ***** AVERAGE HOPCOUNT = %.2f" avgHopCount
                    mailbox.Context.System.Terminate() |> ignore
            | _ -> ()

            return! loop()

        }
    loop ()


let printerRef = spawn chordSystem "PrinterActor" PrinterActor  


let ChordNode (myId:int) (mailbox:Actor<_>) =    
   // let mutable hashSpace = pown(2, m) |> int
    let mutable firstNode = 0
    let mutable mySuccessor = 0
    let mutable mySuccessorRef = null
    let mutable myPredecessor = 0
    let mutable myPredecessorRef = null
    let mutable myFingerTable = []
    let a = FingerTableEntry(0, null)
    let myFingerTable : FingerTableEntry[] = Array.create m a

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()
            let sender = mailbox.Sender()

            match message with 
            | Create (otherId, otherRef) ->
                // First two nodes in the Chord Ring
                mySuccessor <- otherId
                myPredecessor <- otherId
                mySuccessorRef <- otherRef
                myPredecessorRef <- otherRef
                for i in 0..m-1 do
                    let tuple = FingerTableEntry(mySuccessor, mySuccessorRef)
                    myFingerTable.[i] <- tuple
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)

            | Notify(predecessorId, predecessorRef) ->
                //if predecessorId > myPredecessor then
                myPredecessor <- predecessorId
                myPredecessorRef <- predecessorRef

            | FixFingers ->
                let mutable ithFinger = 0
                for i in 1..m-1 do
                    ithFinger <- ( myId + ( pown 2 i ) ) % int(hashSpace)
                    mailbox.Self <! FindithSuccessor(i, ithFinger, mailbox.Self)

            | FindithSuccessor(i, key, tellRef) ->
                if mySuccessor < myId && (key > myId || key < mySuccessor) then
                    tellRef <! FoundFingerEntry(i, mySuccessor, mySuccessorRef)
                elif key <= mySuccessor && key > myId then 
                    tellRef <! FoundFingerEntry(i, mySuccessor, mySuccessorRef)
                //elif myId > key then 
                 //   let ithRef = myFingerTable.[m-1].GetRef()
                //    ithRef <! FindithSuccessor(i, key, tellRef)
                else 
                    let mutable Break = false 
                    let mutable x = m
                    let mutable tempVal = key
                    if myId > key then 
                        tempVal <- key + hashSpace
                    while not Break do
                        x <- x - 1
                        if x < 0 then   
                            mySuccessorRef <! FindithSuccessor(i, key, tellRef)
                            Break <- true
                        else
                            let ithFinger = myFingerTable.[x].GetId()
                            if (ithFinger > myId && ithFinger <= tempVal) then 
                                let ithRef = myFingerTable.[x].GetRef()
                                ithRef <! FindithSuccessor(i, key, tellRef)
                                Break <- true                       
                    done                 

            | FoundFingerEntry(i, fingerId, fingerRef) ->
                let tuple = FingerTableEntry(fingerId, fingerRef)
                myFingerTable.[i] <- tuple

            | Stabilize ->
                if mySuccessor <> 0 then 
                    mySuccessorRef <! PredecessorRequest

            | PredecessorResponse(predecessorOfSuccessor, itsRef) ->                    
                if predecessorOfSuccessor <> myId then
                    mySuccessor <- predecessorOfSuccessor
                    mySuccessorRef <- itsRef
                // Notify mysuccessor
                mySuccessorRef <! Notify(myId, mailbox.Self)
                
            | PredecessorRequest->    
                sender <! PredecessorResponse(myPredecessor, myPredecessorRef)

            | FoundNewNodeSuccessor(isId, isRef) ->
                // Update successor information of self
                mySuccessor <- isId
                mySuccessorRef <- isRef
                // populate fingertable entry with successor - it will get corrected in next FixFingers call
                for i in 0..m-1 do
                    let tuple = FingerTableEntry(mySuccessor, mySuccessorRef)
                    myFingerTable.[i] <- tuple
                // start Stabilize scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(0.0),TimeSpan.FromMilliseconds(StabilizeCycletimeMs), mailbox.Self, Stabilize)
                // start FixFingers scheduler
                chordSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(0.0),TimeSpan.FromMilliseconds(FixFingersCycletimeMs), mailbox.Self, FixFingers)
                // Notify Successor
                mySuccessorRef <! Notify(myId, mailbox.Self)
         
            | KeyLookup(key, hopCount, initiatedBy) ->
                if mySuccessor < myId && (key > myId || key <= mySuccessor) then
                    //printfn "\n iBy = %d key = %d at = %d hc = %d" initiatedBy key mySuccessor hopCount
                    printerRef <! FoundKey(hopCount)
                elif key <= mySuccessor && key > myId then 
                    //printfn "\n iby = %d key = %d at = %d hc = %d" initiatedBy key mySuccessor hopCount
                    printerRef <! FoundKey(hopCount)
                else
                    let mutable Break = false 
                    let mutable x = m
                    let mutable tempVal = key
                    if myId > key then 
                        tempVal <- key + hashSpace
                    while not Break do
                        x <- x - 1
                        if x < 0 then   
                            mySuccessorRef <! KeyLookup(key, hopCount + 1, initiatedBy)
                            Break <- true
                        else
                            let ithFinger = myFingerTable.[x].GetId()
                            if (ithFinger > myId && ithFinger <= tempVal) then 
                                let ithRef = myFingerTable.[x].GetRef()
                                ithRef <! KeyLookup(key, hopCount + 1, initiatedBy)
                                Break <- true                       
                    done 
                
            | StartLookups(numRequests) ->
                //printf "\n %d Starting lookups" myId
                let mutable tempKey = 0
                if mySuccessor <> firstNodeId then 
                    mySuccessorRef <! StartLookups(numRequests)
                for x in 1..numRequests do
                    tempKey <- Random().Next(1, int(hashSpace))
                    mailbox.Self <! KeyLookup(tempKey, 1, myId)
                    //printfn "\n %d req key = %d" myId tempKey
                    System.Threading.Thread.Sleep(800)
            

            | FindNewNodeSuccessor(newId, seekerRef) ->
                if mySuccessor < myId && (newId > myId || newId < mySuccessor) then 
                    seekerRef <! FoundNewNodeSuccessor(mySuccessor, mySuccessorRef)
                    //printfn "\n %d (last node) Successor of %d is %d" myId newId mySuccessor
                elif newId <= mySuccessor && newId > myId then 
                    seekerRef <! FoundNewNodeSuccessor(mySuccessor, mySuccessorRef)
                    //printfn "\n %d Successor of %d is %d" myId newId mySuccessor
                else 
                    mySuccessorRef <! FindNewNodeSuccessor(newId, seekerRef)

            | _ -> ()

            return! loop()
        }
    loop()

let MainActor (mailbox:Actor<_>) =    
    let mutable secondNodeId = 0
    let mutable tempNodeId = 0
    let mutable tempNodeName = ""
    let mutable tempNodeRef = null
    let mutable tempKey = 0
    let list = new List<int>()

    let rec loop () = 
        actor {
            let! (message) = mailbox.Receive()

            match message with 
            | StartAlgorithm(numNodes, numRequests) ->
                firstNodeId <- Random().Next(int(hashSpace))
                printfn "\n\n ADDING %d" firstNodeId
                firstNodeRef <- spawn chordSystem (sprintf "%d" firstNodeId) (ChordNode firstNodeId)
                // Second Node
                secondNodeId <- Random().Next(int(hashSpace))
                printfn "\n\n ADDING %d" secondNodeId
                secondNodeRef <- spawn chordSystem (sprintf "%d" secondNodeId) (ChordNode secondNodeId)
                firstNodeRef <! Create(secondNodeId, secondNodeRef)
                secondNodeRef <! Create(firstNodeId, firstNodeRef)

                for x in 3..numNodes do
                    System.Threading.Thread.Sleep(1000)
                    //tempNodeId <- Random().Next(1, hashSpace)
                    tempNodeId <- [ 1 .. hashSpace ]
                        |> List.filter (fun x -> (not (list.Contains(x))))
                        |> fun y -> y.[Random().Next(y.Length - 1)]
                    list.Add(tempNodeId)
                    printfn "\n\n ADDING %d" tempNodeId
                    tempNodeRef <- spawn chordSystem (sprintf "%d" tempNodeId) (ChordNode tempNodeId)
                    firstNodeRef <! FindNewNodeSuccessor(tempNodeId, tempNodeRef)  
                
                printfn "\n Ring stabilized"
                System.Threading.Thread.Sleep(8000)
                firstNodeRef <! StartLookups(numRequests)

            | _ -> ()

            return! loop()
        }
    loop()


[<EntryPoint>]
let main argv =
    numNodes <-  argv.[0] |> int
    numRequests <- argv.[1] |> int

    mainActorRef <- spawn chordSystem "MainActor" MainActor
    mainActorRef <! StartAlgorithm(numNodes, numRequests)

    chordSystem.WhenTerminated.Wait()
        
    0