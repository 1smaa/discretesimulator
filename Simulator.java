import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.PriorityQueue;

/*
 * POLICY FACOLTATIVA
 * La policy facoltativa si basa su una priority queue per determinare il server con meno job all'interno della sua coda FIFO ( o in esecuzione ).
 * Ogni qual volta che viene inserito o tolto un job la priority queue viene aggiornata. L'idea è che questo crei una distribuzione
 * più uniforme dei job sui vari server dando vita ad un ETA e AQT minori. Questo non è vero nel caso ETA sia limitato dal tempo
 * di arrivo dell'N-esimo Job, con il server di arrivo in entrambe le policy vuoto. Il cambiamento sul tempo medio di coda è in ogni caso apprezzabile.
 */

public class Simulator {
    public static class Category {
        private RandomGenerator arrivalGen; // random generator of interarrival times
        private RandomGenerator serviceGen; // random generator of service times
        public int id; // category id
    
        public Category(RandomGenerator arrivalGen,RandomGenerator serviceGen,int id){
            this.arrivalGen=arrivalGen;
            this.serviceGen=serviceGen;
            this.id=id;
        }
        
        //generate new interarrival time
        public double generateArrival(){
            return this.arrivalGen.generate(); 
        }
    
        //generate new service time
        public double generateService(){
            return this.serviceGen.generate();
        }
    }
    
    // Entry class for JobPriorityQueue
    public static class Entry{
        public Entry(double key,Job value){
            this.key=key;
            this.value=value;
            this.type=0;
        }

        public Entry(double key,Job value,int type){
            this.key=key;
            this.value=value;
            this.type=type;
        }
        public double getKey(){
            return this.key;
        }
        public Job getValue(){
            return this.value;
        }
        private double key;
        private Job value;
        public int type;
    }
    
    public static class Job {
        public Job(int id,double arrivalTime,Category c){
            this.id=id;
            this.arrivalTime=arrivalTime;
            this.category=c;
        }
        public int getId(){ return this.id; }
        public double getService(){ return this.serviceTime; }
        public double getArrival(){ return this.arrivalTime; }
        public Category category(){ return this.category; }
        public void setService(double time){ this.serviceTime=time; }

        private int id;
        private double serviceTime;
        private double arrivalTime;
        private Category category;
        public int server;
    }

    // Assign jobs to servers following 1 of 2 policies
    public static class Policy {
        public Policy(Server[] servers,int K,int p){
            this.K=K;
            this.p=p;
            this.servers=servers;
            this.serverPriority=new PriorityQueue<>();
            for(int i=0;i<K;i++){
                serverPriority.add(this.servers[i]);
            }
        }

        public void update(int i){
            serverPriority.remove(servers[i]);
            serverPriority.add(servers[i]);
        }

        public int assign(int i){
            if(p==0){
                return this.required(i);
            } else {
                return this.optional(i);
            }
        }

        private int required(int i){
            return i%this.K;
        }

        //find the server with a shorter FIFO queue
        private int optional(int i){
            return this.serverPriority.peek().id;
        }
        private int K;
        private int p;
        private PriorityQueue<Server> serverPriority;
        private Server[] servers;
    }
    
    // JobPriorityQueue implementation
    public static class JobPriorityQueue {
        private Entry[] q;
        private int size;
        private static final int INITSIZE = 10;
    
        public JobPriorityQueue() {
            this.q = new Entry[INITSIZE];
            this.size = 0;
        }
    
        public JobPriorityQueue(int size) {
            this.q = new Entry[size];
            this.size = 0;
        }
    
        public JobPriorityQueue(Entry[] arr) {
            this.q = arr;
            this.size = arr.length;
            for (int j = (this.size - 1) / 2; j >= 0; j--) {
                this.downHeapBubbling(j);
            }
        }
    
        public Entry insert(Entry e) {
            if (this.size == q.length) {
                ensureCapacity();
            }
            this.q[size] = e;
            this.size++;
            upHeapBubbling(this.size - 1);
            return e;
        }
    
        public Entry top() {
            if (this.size > 0) {
                return this.q[0];
            } else {
                return null;
            }
        }
    
        public Entry pop() {
            if (this.size > 0) {
                Entry min = this.q[0];
                this.size--;
                this.q[0] = this.q[this.size];
                this.downHeapBubbling(0);
                return min;
            }
            return null;
        }
    
        public int size() {
            return this.size;
        }
    
        public boolean isEmpty() {
            return this.size == 0;
        }
    
        private void downHeapBubbling(int i) {
            int j = this.indexMinChild(i);
            while (j != -1 && this.q[i].getKey()>this.q[j].getKey()) {
                swap(i, j);
                i = j;
                j = this.indexMinChild(j);
            }
        }
    
        private void upHeapBubbling(int i) {
            int j = (i - 1) / 2;
            while (j >= 0 && this.q[j].getKey()>this.q[i].getKey()) {
                swap(i, j);
                i = j;
                j = (i - 1) / 2;
            }
        }
    
        private int indexMinChild(int i) {
            int leftChild = (2 * i) + 1;
            int rightChild = (2 * i) + 2;
            if (rightChild >= this.size && leftChild >= this.size) {
                return -1;
            }
            if (rightChild >= this.size || this.q[leftChild].getKey()<this.q[rightChild].getKey()) {
                return leftChild;
            }
            return rightChild;
        }
    
        private void swap(int i, int j) {
            Entry aux = this.q[i];
            this.q[i] = this.q[j];
            this.q[j] = aux;
        }
    
        private void ensureCapacity() {
            int newCapacity = q.length * 2;
            Entry[] nq=new Entry[newCapacity];
            System.arraycopy(this.q,0,nq,0,this.size);
            this.q=nq;
        }
    }
    
    // random generator
    public static class RandomGenerator {
        public RandomGenerator(int seed,float lambda){
            this.generator=new Random(seed);
            this.lambda=lambda;
        }
        public double generate(){
            return -Math.log(1-this.generator.nextFloat())/this.lambda;
        }
        private float lambda;
        private Random generator;
    }
    
    // server class with fifo queue
    public static class Server implements Comparable<Server>{
        public Server(int id){
            this.id=id;
            this.q=new LinkedList<>();
            this.executing=null;
        }

        //returns true if the FIFO queue is empty, but returns true even if a job is executing
        public boolean isFree(){
            return this.q.isEmpty();
        }

        //returns the job that is being executed
        public Job executing(){
            return this.executing;
        }

        //return true if the server is executing a job, false otherwise
        public boolean busy(){
            return this.executing!=null;
        }

        //adds to the fifo queue if the server is busy, puts in execution otherwise
        public void enqueue(Job j){
            if(this.executing()==null){
                this.executing=j;
            } else {
                this.q.add(j);
            }
        }

        //takes the job that was being executed out, and if the FIFO is not empty puts the next one in execution
        public Job dequeue(){
            Job j=this.executing;
            if(!this.q.isEmpty()){
                this.executing=this.q.poll();
            } else {
                this.executing=null;
            }
            return j;
        }

        //returns the number of jobs in the server, containing the one execution and the length of the FIFO queue
        public int size(){
            int s;
            if(this.busy()){
                s=1;
            } else {
                s=0;
            }
            return s+this.q.size();
        }

        @Override
        public int compareTo(Server other){
            return Integer.compare(this.size(), other.size());
        }

        private Queue<Job> q;
        private Job executing;
        public int id;
    }    
    public static void main(String[] args){
        String file=args[0];
        try{
            // create file reader instance
            FileReader fr=new FileReader(file);
            Scanner scan=new Scanner(fr);
            String line=scan.nextLine(); //scan the first line
            Scanner lineScanner=new Scanner(line);
            System.out.println(line); // immediately print the first line as it is
            lineScanner.useDelimiter(",");
            final int K=lineScanner.nextInt(); // number of servers
            final int H=lineScanner.nextInt(); // number of job categories
            final int N=lineScanner.nextInt(); // number of jobs in the simulation
            final int R=lineScanner.nextInt(); // number of runs (or repetitions) of the simulation
            final int P=lineScanner.nextInt(); // 1 for optional policy, 0 for required policy 
            int j=0;
            Category[] categories=new Category[H]; // creates array of categories
            while(scan.hasNextLine()){
                lineScanner=new Scanner(scan.nextLine());
                lineScanner.useDelimiter(",");
                float lambdaArr=Float.parseFloat(lineScanner.next()); //lambda for arrival time generator
                float lambdaSer=Float.parseFloat(lineScanner.next()); //lambda for service time generator
                int seedArr=lineScanner.nextInt(); //seed for arrival time generator
                int seedSer=lineScanner.nextInt(); //lambda for service time generator
                RandomGenerator arrGen=new RandomGenerator(seedArr, lambdaArr); //declaration of the generators for category j
                RandomGenerator serGen=new RandomGenerator(seedSer, lambdaSer);
                categories[j]=new Category(arrGen, serGen, j);
                j++;
            }
            Server[] servers=new Server[K]; // server array declaration
            double eqa=0; //end time of the simulation ( mean on R runs)
            double aqtall=0; // average queueing time of the jobs ( mean on R runs )
            double[][] data=new double[H][3]; // collects data reguarding each category for each run
            double[][] cat=new double[H][3]; // collects data reguarding each category ( mean on R runs )
            for(int i=0;i<H;i++){ // initialize the arrays
                for(int i1=0;i1<3;i1++){
                    data[i][i1]=0;
                    cat[i][i1]=0;
                }
            }
            int id=0; // job id to keep track of jobs order of arrival
            for(int k=0;k<R;k++){
                for(int i=0;i<H;i++){ // zero-out the data array
                    for(int i1=0;i1<3;i1++){
                        data[i][i1]=0;
                    }
                }
                double runEqa=0; // eqa of the current run
                double runAqtAll=0; // aqt of the current run
                JobPriorityQueue pq=new JobPriorityQueue(); // priority queue declaration
                for(int i=0;i<K;i++){ // server array initialization
                    servers[i]=new Server(i);
                }
                Policy p=new Policy(servers,K, P); // policy declaration
                int n=0;
                for(int i=0;i<H;i++){ // generate one arrival for each category
                    double t=categories[i].generateArrival(); // generate arrival
                    pq.insert(new Entry(t,new Job(id++,t,categories[i]))); // insert job
                }
                while(!pq.isEmpty()){ // while there are events
                    Entry e=pq.pop(); // get entry
                    Job job=e.getValue(); // get job
                    double t=e.getKey(); // get time of the event
                    Job svc=null; // job linked to information to be printed in case some conditions are met
                    if(e.type==0){ // if the event is the arrival of a job
                        if(++n>N){ // if N jobs are already arrived, ignore and go to the next event
                            continue; 
                        }
                        double arrival=job.category().generateArrival(); // generate new arrival for the same category
                        Job newJob=new Job(id++,t+arrival,job.category()); // create  the job
                        Entry newE=new Entry(t+arrival,newJob,0); // create the entry of type 0
                        pq.insert(newE); // insert in priority queue
                        int s=p.assign(id++); // assign the job to a server
                        job.server=s;
                        if(!servers[job.server].busy()){ // if the server is not busy
                            double service=job.category().generateService(); // create immediately the end event
                            Entry nE=new Entry(t+service,job,1);
                            job.setService(service);
                            data[job.category().id][2]+=service; // increment metrics
                            data[job.category().id][0]++;
                            pq.insert(nE);
                        }
                        servers[job.server].enqueue(job); // enqueue the job
                        if(P==1){
                            p.update(job.server);
                        }
                    } else { // if the job is the end of an event
                        svc=servers[job.server].dequeue(); // dequeue the job
                        if(P==1){
                            p.update(job.server);
                        }
                        if(servers[job.server].busy()){ // if a job has been put in execution
                            Job endJob=servers[job.server].executing(); // get said job
                            double service=endJob.category().generateService(); // generate end event for this job
                            endJob.setService(service);
                            Entry nE=new Entry(t+service, endJob,1);
                            pq.insert(nE);
                            double qTime=(t-endJob.getArrival());
                            runAqtAll+=qTime/N; // increase metrics
                            data[endJob.category().id][1]+=qTime;
                            data[endJob.category().id][2]+=service;
                            data[endJob.category().id][0]++;
                        }
                        runEqa=t;
                    }
                    if(R==1&&N<=10&&P==0){ // if the input respects some criteria print additional information
                        System.out.print(t+",");
                        if(e.type==0){
                            System.out.print("0.0");
                        } else {
                            System.out.print((svc.getService()));
                        }
                        System.out.print(","+job.category().id+"\n");
                    }
                }
                eqa+=runEqa/R; // add the run parameters to the global parameters
                aqtall+=runAqtAll/R;
                for(int i=0;i<H;i++){
                    cat[i][0]+=(data[i][0]/R);
                    cat[i][1]+=(data[i][1]/data[i][0])/R;
                    cat[i][2]+=(data[i][2]/data[i][0])/R;
                }
            }
            System.out.print((eqa)+"\n"+(aqtall)+"\n"); // print the information
            for(int i=0;i<H;i++){
                System.out.print((cat[i][0])+","+(cat[i][1])+","+(cat[i][2])+"\n");
            }
            scan.close();
        }catch(FileNotFoundException e){
            System.out.println("File non trovato.\n");
        }
    }
}
