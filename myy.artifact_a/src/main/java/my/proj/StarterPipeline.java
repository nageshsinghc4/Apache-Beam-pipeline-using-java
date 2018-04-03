package my.proj;

import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import avro.shaded.com.google.common.collect.ImmutableList;

import com.pojo.ClassBrands;
import com.pojo.ClassWeeklyDueto;
import com.pojo.ClassWeeks;
import com.pojo.ClassEvents;
import com.pojo.ClassShip;
import com.pojo.ClassFinance;
import com.pojo.ClassSpend;
import com.pojo.ClassCurves;
import com.pojo.ClassxNorms;
import com.pojo.ClassBasis;
import com.pojo.ClassxNormCamp;
import com.pojo.MyOutputClass;

public class StarterPipeline {

	public static void main(String[] args) {
		SimpleDateFormat date1 = new SimpleDateFormat("MM/dd/yyyy");
	
		DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		 options.setProject("beta-194409");
		 options.setStagingLocation("gs://cloroxtegbucket/staging");
		// options.setGcpTempLocation("gs://cloroxtegbucket/tmp");
		 options.setRunner(DataflowRunner.class);
    	 DataflowRunner.fromOptions(options);
         Pipeline p = Pipeline.create(options);
         
    	// implement WeeklyDueto.csv
		PCollection<String> weekly = p.apply(TextIO.read().from("gs://cloroxtegbucket/input/WeeklyDueto.csv"));	

		PCollection<ClassWeeklyDueto> pojos = weekly.apply(ParDo.of(new DoFn<String, ClassWeeklyDueto>() { // converting String into class
																						
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext c) throws ParseException {			
			String[] strArr = c.element().split(",");
			double number = Double.parseDouble(strArr[5]);
		
     		ClassWeeklyDueto clorox = new ClassWeeklyDueto();
				clorox.setCatLib(strArr[1]);
				clorox.setCausalValue(Double.valueOf(strArr[7]));
				clorox.setDuetoValue(number);
				clorox.setModelIteration(Double.valueOf(strArr[8]));
				clorox.setOutlet(strArr[0]);
				clorox.setPrimaryCausalKey(strArr[6]);
				clorox.setProdKey(strArr[2]);
				clorox.setPublished(Double.valueOf(strArr[9]));
				clorox.setSalesComponent(strArr[4]);
				clorox.setWeek(date1.parse(strArr[3]));
				c.output(clorox);
			}
		}));


		BeamRecordSqlType appType = BeamRecordSqlType.create(
				  Arrays.asList("Outlet", "CatLib", "ProdKey", "Week", "SalesComponent", "DuetoValue","PrimaryCausalKey", "CausalValue", "ModelIteration", "Published"),
				  Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE));
		
	/*	List<String> fieldNames = Arrays.asList("Outlet", "CatLib", "ProdKey", "Week", "SalesComponent", "DuetoValue",
				"PrimaryCausalKey", "CausalValue", "ModelIteration", "Published");
		List<Integer> fieldTypes = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE,
				Types.VARCHAR, Types.FLOAT, Types.VARCHAR, Types.FLOAT, Types.INTEGER, Types.INTEGER);
		final BeamRecordSqlType appType = BeamRecordSqlType.create(fieldNames, fieldTypes);
*/

		PCollection<BeamRecord> apps = pojos.apply(ParDo.of(new DoFn<ClassWeeklyDueto, BeamRecord>() {
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType, c.element().Outlet, c.element().CatLib, c.element().ProdKey,
						c.element().Week, c.element().SalesComponent, c.element().DuetoValue,
						c.element().PrimaryCausalKey, c.element().CausalValue, c.element().ModelIteration,
						c.element().Published);
				c.output(br);
				 }
		})).setCoder(appType.getRecordCoder());
		
			 
	
		// implement Brands.csv
		PCollection<String> lines =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/Brands.csv"));
		PCollection<ClassBrands> pojos1 = lines.apply(ParDo.of(new DoFn<String, ClassBrands>() { // converting String into  class typ								
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext c) {
				String[] strArr = c.element().split(",");
				ClassBrands classbrands = new ClassBrands();
				classbrands.setSubBrand(strArr[0]);
				classbrands.setBrandName(strArr[1]);
				classbrands.setCatLib(strArr[2]);
				classbrands.setProdKey(strArr[3]);
				classbrands.setCatLibProdKey(strArr[4]);
				c.output(classbrands);
			}
		}));

		List<String> fieldNames1 = Arrays.asList("SubBrand", "BrandName", "CatLib", "ProdKey", "CatLibProdKey");
		List<Integer> fieldTypes1 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR);
		final BeamRecordSqlType appType1 = BeamRecordSqlType.create(fieldNames1, fieldTypes1);

		PCollection<BeamRecord> apps1 = pojos1.apply(ParDo.of(new DoFn<ClassBrands, BeamRecord>() {
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext c) {
				BeamRecord br = new BeamRecord(appType1, c.element().SubBrand, c.element().BrandName,
						c.element().CatLib, c.element().ProdKey, c.element().CatLibProdKey);
				c.output(br);
			}
		})).setCoder(appType1.getRecordCoder());

		// implement Weeks.csv
				PCollection<String> weekobj =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/Weeks.csv"));
				PCollection<ClassWeeks> pojos2 = weekobj.apply(ParDo.of(new DoFn<String, ClassWeeks>() { // converting String into class
																								// typ
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) throws ParseException {
					//SimpleDateFormat date2 = new SimpleDateFormat("MM/dd/yyyy");
						String[] strArr = c.element().split(",");
						ClassWeeks cw = new ClassWeeks();
						cw.setQuarter(strArr[0]);
						cw.setWeek(date1.parse(strArr[1]));
						cw.setFinancialYear(strArr[2]);
						cw.setCurrentYear(strArr[3]);
						c.output(cw);
					}
				}));

				List<String> fieldNames2 = Arrays.asList("Quarter", "Week", "FinancialYear", "CurrentYear");
				List<Integer> fieldTypes2 = Arrays.asList(Types.VARCHAR, Types.DATE, Types.VARCHAR, Types.VARCHAR);
				final BeamRecordSqlType appType2 = BeamRecordSqlType.create(fieldNames2, fieldTypes2);

				PCollection<BeamRecord> apps2 = pojos2.apply(ParDo.of(new DoFn<ClassWeeks, BeamRecord>() {
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						BeamRecord br = new BeamRecord(appType2, c.element().Quarter, c.element().Week, c.element().FinancialYear, c.element().CurrentYear);
						c.output(br); 
						}
				})).setCoder(appType2.getRecordCoder());
						
				// implement Events.csv
				PCollection<String> eventobj =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/Events.csv"));	
				PCollection<ClassEvents> pojos3 = eventobj.apply(ParDo.of(new DoFn<String, ClassEvents>() { // converting String into class
																							
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						String[] strArr = c.element().split(",");
						ClassEvents ce = new ClassEvents();
						ce.setEventList(strArr[0]);
						ce.setEventDescription(strArr[1]);
						ce.setVehicle(strArr[2]);
						c.output(ce);
					}
				}));

				List<String> fieldNames3 = Arrays.asList("EventList", "EventDescription", "Vehicle");
				List<Integer> fieldTypes3 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR);
				final BeamRecordSqlType appType3 = BeamRecordSqlType.create(fieldNames3, fieldTypes3);

				PCollection<BeamRecord> apps3 = pojos3.apply(ParDo.of(new DoFn<ClassEvents, BeamRecord>() {
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						BeamRecord br = new BeamRecord(appType3, c.element().EventList, c.element().EventDescription, c.element().Vehicle);
						c.output(br); 
						}
				})).setCoder(appType3.getRecordCoder());
				
      		// implement Shipment.csv
				PCollection<String> shipobj =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/Shipment2.csv"));		
				PCollection<ClassShip> pojos4 = shipobj.apply(ParDo.of(new DoFn<String, ClassShip>() { // converting String into classType
																								
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
					
						String[] strArr = c.element().split(",");
						ClassShip cship = new ClassShip();
						cship.setBrand(strArr[0]);
						cship.setSubBrand(strArr[1]);
						cship.setCatLib(strArr[2]);
						cship.setChannel(strArr[3]);
						cship.setPeriod2(strArr[4]);
						cship.setChannelVolume(strArr[5]);
						cship.setAllOutletVolume(strArr[6]);
						cship.setProjectionFactor(Double.parseDouble(strArr[7]));
						c.output(cship);
					}
				}));

				List<String> fieldNames4 = Arrays.asList("Brand", "SubBrand", "CatLib", "Channel", "Period2", "ChannelVolume", "AllOutletVolume", "ProjectionFactor");
				List<Integer> fieldTypes4 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR, Types.DOUBLE);
				final BeamRecordSqlType appType4 = BeamRecordSqlType.create(fieldNames4, fieldTypes4);

				PCollection<BeamRecord> apps4 = pojos4.apply(ParDo.of(new DoFn<ClassShip, BeamRecord>() {
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						BeamRecord br = new BeamRecord(appType4, c.element().Brand, c.element().SubBrand, c.element().CatLib,
						c.element().Channel, c.element().Period2, c.element().ChannelVolume, c.element().AllOutletVolume, c.element().ProjectionFactor);
						c.output(br); }
				})).setCoder(appType4.getRecordCoder());
			    PCollection<BeamRecord> ship_trig = apps4.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

				
				// implement Finance.csv
				PCollection<String> financeobj =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/Financials.csv"));		
				PCollection<ClassFinance> pojos5 = financeobj.apply(ParDo.of(new DoFn<String, ClassFinance>() { // converting String into classtype
																								
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						String[] strArr = c.element().split(",");
						ClassFinance ce = new ClassFinance();
						ce.setSubbrandName(strArr[0]);
						ce.setBrandName(strArr[1]);
						ce.setPeriod3(strArr[2]);
						ce.setPplScVolume(Double.valueOf(strArr[3]));
						ce.setTlPerSc(Double.valueOf(strArr[4]));
						ce.setNetRealPerSc(Double.valueOf(strArr[5]));
						ce.setCpfPerSc(Double.valueOf(strArr[6]));
						ce.setNcsPerSc(Double.valueOf(strArr[7]));
						ce.setContribPerSc(Double.valueOf(strArr[8]));
						ce.setAdjContribPerSc(Double.valueOf(strArr[9]));
						ce.setTL(Double.valueOf(strArr[10]));
						ce.setNetReal(Double.valueOf(strArr[11]));
						ce.setCPF(Double.valueOf(strArr[12]));
						ce.setNCS(Double.valueOf(strArr[13]));
						ce.setContrib(Double.valueOf(strArr[14]));
						ce.setAdjContrib(Double.valueOf(strArr[15]));
						c.output(ce);
					}
				}));

				List<String> fieldNames5 = Arrays.asList("SubbrandName", "BrandName", "Period3", "PplScVolume", "TlPerSc",
				 "NetRealPerSc", "CpfPerSc", "NcsPerSc", "ContribPerSc", "AdjContribPerSc", "TL", "NetReal", "CPF", "NCS",
				  "Contrib", "AdjContrib");
				List<Integer> fieldTypes5 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE,
						Types.DOUBLE, Types.DOUBLE,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE);
				final BeamRecordSqlType appType5 = BeamRecordSqlType.create(fieldNames5, fieldTypes5);

				PCollection<BeamRecord> apps5 = pojos5.apply(ParDo.of(new DoFn<ClassFinance, BeamRecord>() {
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						BeamRecord br = new BeamRecord(appType5, c.element().SubbrandName, c.element().BrandName, c.element().Period3,
						c.element().PplScVolume, c.element().TlPerSc, c.element().NetRealPerSc, c.element().CpfPerSc,
						c.element().NcsPerSc,  c.element().ContribPerSc, c.element().AdjContribPerSc, c.element().TL, 
						c.element().NetReal, c.element().CPF, c.element().NCS,
						c.element().Contrib, c.element().AdjContrib);
						c.output(br); }
				})).setCoder(appType5.getRecordCoder());
			    PCollection<BeamRecord> finance_trig = apps5.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

				
				// implement Spend.csv
				PCollection<String> spendobj =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/Spend.csv"));		
				PCollection<ClassSpend> pojos6 = spendobj.apply(ParDo.of(new DoFn<String, ClassSpend>() { // converting String into class
																								// typ
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						String[] strArr = c.element().split(",");
						ClassSpend ce = new ClassSpend();
						ce.setVehicle(strArr[0]);
						ce.setCopy(strArr[1]);
						ce.setPrincipalBrand(strArr[2]);
						ce.setFiscalYear(strArr[3]);
						ce.setFiscalQuarter(strArr[4]);
						ce.setCalendarYear(strArr[5]);
						ce.setCalendarQuarter(strArr[6]);
						ce.setSpend(Double.parseDouble(strArr[7]));
					}
				}));

				List<String> fieldNames6 = Arrays.asList("Vehicle", "Copy", "PrincipalBrand", "FiscalYear", "FiscalQuarter", "CalendarYear", "CalendarQuarter", "Spend");
				List<Integer> fieldTypes6 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR, Types.DOUBLE);
				final BeamRecordSqlType appType6 = BeamRecordSqlType.create(fieldNames6, fieldTypes6);

				PCollection<BeamRecord> apps6 = pojos6.apply(ParDo.of(new DoFn<ClassSpend, BeamRecord>() {
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						BeamRecord br = new BeamRecord(appType4, c.element().Vehicle, c.element().Copy, c.element().PrincipalBrand,
						c.element().FiscalYear, c.element().FiscalQuarter, c.element().CalendarYear, c.element().CalendarQuarter,
						c.element().Spend);
						c.output(br); }
				})).setCoder(appType6.getRecordCoder());
				 PCollection<BeamRecord> spend_trig = apps6.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

				
				// implement Curves.csv
				PCollection<String> curveobj =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/Curves.csv"));		
				PCollection<ClassCurves> pojos7 = curveobj.apply(ParDo.of(new DoFn<String, ClassCurves>() { // converting String into class type
																								
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						String[] strArr = c.element().split(",");
						ClassCurves cc = new ClassCurves();
						cc.setBrand(strArr[0]);
						cc.setSBU(strArr[1]);
						cc.setDivision(strArr[2]);
						cc.setVehicle(strArr[3]);
						cc.setAlpha(Double.parseDouble(strArr[4]));
						cc.setBeta(Double.parseDouble(strArr[5]));
						c.output(cc);
					}
				}));

				List<String> fieldNames7 = Arrays.asList("Brand", "SBU", "Division", "Vehicle", "Alpha","Beta");
				List<Integer> fieldTypes7 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.VARCHAR, Types.DOUBLE, Types.DOUBLE);
				final BeamRecordSqlType appType7 = BeamRecordSqlType.create(fieldNames7, fieldTypes7);

				PCollection<BeamRecord> apps7 = pojos7.apply(ParDo.of(new DoFn<ClassCurves, BeamRecord>() {
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						BeamRecord br = new BeamRecord(appType7, c.element().Brand, c.element().SBU, c.element().Division,
						c.element().Vehicle, c.element().Alpha, c.element().Beta);
						c.output(br); }
				})).setCoder(appType7.getRecordCoder());
			    PCollection<BeamRecord> curve_trig = apps7.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

			
				// implement xNormInput.csv
				PCollection<String> xNormobj =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/xNorminput1.csv"));		
				PCollection<ClassxNorms> pojos8 = xNormobj.apply(ParDo.of(new DoFn<String, ClassxNorms>() { // converting String into class
																								// typ
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) throws ParseException {
						String[] strArr = c.element().split(",");
						ClassxNorms xn = new ClassxNorms();
						xn.setVehicle(strArr[0]);
						xn.setPrincipalBrand(strArr[1]);
						xn.setPeriod(strArr[2]);
						xn.setPeriodStartWeek(date1.parse(strArr[3]));
						xn.setPeriodEndWeek(date1.parse(strArr[4]));
						xn.setGRPs(Double.parseDouble(strArr[5]));
						xn.setDuration(Double.parseDouble(strArr[6]));
						xn.setContinuity(Double.parseDouble(strArr[7]));
						xn.setBasisPeriod(strArr[8]);
						xn.setBasis(Double.parseDouble(strArr[9]));
						xn.setBasisDuration(Double.parseDouble(strArr[10]));
						xn.setAlpha(Double.parseDouble(strArr[11]));
						xn.setBeta(Double.parseDouble(strArr[12]));
						xn.setVolume(Double.parseDouble(strArr[13]));
						xn.setSpend(Double.parseDouble(strArr[14]));
						xn.setEfficiency(Double.parseDouble(strArr[15]));
						xn.setXnorm(Double.parseDouble(strArr[16]));
						xn.setKey(strArr[17]);
						c.output(xn);
					}
				}));

				List<String> fieldNames8 = Arrays.asList("Vehicle", "PrincipalBrand", "Period", "PeriodStartWeek", "PeriodEndWeek","GRPs",
				"Duration", "Continuity", "BasisPeriod", "Basis", "BasisDuration","Alpha","Beta", "Volume", "Spend", "Efficiency","Xnorm","Key");
				List<Integer> fieldTypes8 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.DATE, Types.DOUBLE, Types.DOUBLE,
				Types.DOUBLE,Types.VARCHAR, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.VARCHAR);
				final BeamRecordSqlType appType8 = BeamRecordSqlType.create(fieldNames8, fieldTypes8);

				PCollection<BeamRecord> apps8 = pojos8.apply(ParDo.of(new DoFn<ClassxNorms, BeamRecord>() {
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						BeamRecord br = new BeamRecord(appType8, c.element().Vehicle, c.element().PrincipalBrand, c.element().Period,
						c.element().PeriodStartWeek, c.element().PeriodEndWeek, c.element().GRPs,  c.element().Duration, 
						c.element().Continuity, c.element().BasisPeriod, c.element().Basis, c.element().BasisDuration, c.element().Alpha,
						c.element().Beta, c.element().Volume, c.element().Spend,
						c.element().Efficiency, c.element().Xnorm, c.element().Key);
						c.output(br);
						}
				})).setCoder(appType8.getRecordCoder());
			    PCollection<BeamRecord> xnorm_trig = apps8.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

			   

				// implement Basis.csv
				PCollection<String> basisobj =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/Basis.csv"));		
				PCollection<ClassBasis> pojos9 = basisobj.apply(ParDo.of(new DoFn<String, ClassBasis>() { // converting String into class
																								// typ
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						String[] strArr = c.element().split(",");
						ClassBasis cb = new ClassBasis();
						cb.setVehicle(strArr[0]);
						cb.setBasis(Double.parseDouble(strArr[1]));
						cb.setBasisDuration(Double.parseDouble(strArr[2]));
						c.output(cb);
					}
				}));

				List<String> fieldNames9 = Arrays.asList("Vehicle", "Basis", "BasisDuration");
				List<Integer> fieldTypes9 = Arrays.asList(Types.VARCHAR, Types.DOUBLE, Types.DOUBLE);
				final BeamRecordSqlType appType9 = BeamRecordSqlType.create(fieldNames9, fieldTypes9);

				PCollection<BeamRecord> apps9 = pojos9.apply(ParDo.of(new DoFn<ClassBasis, BeamRecord>() {
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						BeamRecord br = new BeamRecord(appType9, c.element().Vehicle, c.element().Basis, c.element().BasisDuration);
						c.output(br); }
				})).setCoder(appType9.getRecordCoder());
			    PCollection<BeamRecord> basis_trig = apps9.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	
				
				// implement xNormInputCampaign.csv
				PCollection<String> xNormCampobj =p.apply(TextIO.read().from("gs://cloroxtegbucket/input/xNormInputCampaign.csv"));		
				PCollection<ClassxNormCamp> pojos10 = xNormCampobj.apply(ParDo.of(new DoFn<String, ClassxNormCamp>() { // converting String into class
																								// typ
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) throws ParseException {
						String[] strArr = c.element().split(",");
						ClassxNormCamp ce = new ClassxNormCamp();
						ce.setVehicle(strArr[0]);
						ce.setCampaign(strArr[1]);
						ce.setCopy(strArr[2]);
						ce.setPrincipalBrand(strArr[3]);
						ce.setPeriod(strArr[4]);
						ce.setPeriodStartDate(date1.parse(strArr[5]));
						ce.setPeriodEndDate(date1.parse(strArr[6]));
						ce.setGRPs(Double.parseDouble(strArr[7]));
						ce.setDuration(Double.parseDouble(strArr[8]));
						ce.setContinuity(Double.parseDouble(strArr[9]));
						ce.setBasis(Double.parseDouble(strArr[10]));
						ce.setBasisDuration(Double.parseDouble(strArr[11]));
						ce.setAlpha(Double.parseDouble(strArr[12]));
						ce.setBeta(Double.parseDouble(strArr[13]));
						ce.setVolume(Double.parseDouble(strArr[14]));
						ce.setSpend(Double.parseDouble(strArr[15]));
						ce.setEfficiency(Double.parseDouble(strArr[16]));
						ce.setXnorm(Double.parseDouble(strArr[17]));
						ce.setKey(strArr[18]);
						c.output(ce);
					}
				}));

				List<String> fieldNames10 = Arrays.asList("Vehicle", "Campaign", "Copy", "PrincipalBrand", "Period","PeriodStartDate","PeriodEndDate","GRPs",
						"Duration", "Continuity", "Basis", "BasisDuration", "Alpha", "Beta", "Volume", "Spend", "Efficiency","Xnorm","Key");
				List<Integer> fieldTypes10 = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.DATE,
						Types.DOUBLE,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,Types.DOUBLE, Types.VARCHAR);
				final BeamRecordSqlType appType10 = BeamRecordSqlType.create(fieldNames10, fieldTypes10);

				PCollection<BeamRecord> apps10 = pojos10.apply(ParDo.of(new DoFn<ClassxNormCamp, BeamRecord>() {
					private static final long serialVersionUID = 1L;
					@ProcessElement
					public void processElement(ProcessContext c) {
						BeamRecord br = new BeamRecord(appType10, c.element().Vehicle, c.element().Campaign, c.element().Copy, c.element().PrincipalBrand, c.element().Period, c.element().PeriodStartDate
								,c.element().PeriodEndDate, c.element().GRPs, c.element().Duration, c.element().Continuity, c.element().Basis,
								c.element().BasisDuration, c.element().Alpha, c.element().Beta, c.element().Volume, c.element().Spend,
								c.element().Efficiency, c.element().Xnorm, c.element().Key);
						c.output(br); }
				})).setCoder(appType10.getRecordCoder());		
				PCollection<BeamRecord> xcam_trig = apps10.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

			    											//Queries starts from here 	
	
		PCollection<BeamRecord> first = apps.apply(BeamSql.query(
				"SELECT Outlet, CatLib, ProdKey, Week, SalesComponent, DuetoValue, PrimaryCausalKey, CausalValue, ModelIteration, Published, (CatLib ||'.'||ProdKey) as CatLibKey from PCOLLECTION"));
		
		//creating tupletag to insert data from two data sets
		//Query 1
		PCollectionTuple query1 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("first"), first).and(new TupleTag<BeamRecord>("apps1"), apps1);	    
		PCollection<BeamRecord> record1 = query1.apply(
		BeamSql.queryMulti("SELECT a.Outlet, a.CatLib, a.ProdKey, a.Week, a.SalesComponent, a.DuetoValue, a.PrimaryCausalKey, a.CausalValue, a.ModelIteration, a.Published, a.CatLibKey, b.SubBrand, b.BrandName from first a LEFT JOIN apps1 b on a.CatLibKey = b.CatLibProdKey"));	
		
		//Query2
		PCollectionTuple query2 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record1"), record1).and(new TupleTag<BeamRecord>("apps2"), apps2);	    
		PCollection<BeamRecord> record2 = query2.apply(
		BeamSql.queryMulti("SELECT a.Outlet, a.CatLib, a.ProdKey, a.Week, cast(cast(EXTRACT(YEAR from a.Week) as VARCHAR) || (case when EXTRACT(MONTH from a.Week) < 10 then '0' || cast(EXTRACT(MONTH from a.Week) as VARCHAR) else cast(EXTRACT(MONTH from a.Week) as VARCHAR) END) || (case when EXTRACT(DAY from a.Week) < 10 then '0' || cast(EXTRACT(DAY from a.Week) as VARCHAR) else  cast(EXTRACT(DAY from a.Week) as VARCHAR) END) as BIGINT) as Week1, a.SalesComponent, a.DuetoValue, a.PrimaryCausalKey, a.CausalValue, a.ModelIteration, a.Published,  a.CatLibKey, a.SubBrand, a.BrandName, b.FinancialYear FROM record1 a LEFT JOIN apps2 b on a.Week = b.Week"));

	 	//Query3
		PCollectionTuple query3 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record2"), record2).and(new TupleTag<BeamRecord>("apps3"), apps3);	    
		PCollection<BeamRecord> record3 = query3.apply(
				BeamSql.queryMulti("SELECT a.Outlet, a.CatLib, a.ProdKey, a.Week1, a.SalesComponent, a.DuetoValue, a.PrimaryCausalKey, a.CausalValue, a.ModelIteration, a.Published, a.CatLibKey, a.SubBrand, a.BrandName, a.FinancialYear, b.EventDescription, b.Vehicle FROM record2 a LEFT JOIN apps3 b on a.SalesComponent = b.EventList WHERE b.Vehicle IS NOT NULL"));
		PCollection<BeamRecord> record3_trig = record3.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());
			//	BeamSql.queryMulti("SELECT a.Outlet, a.CatLib, a.ProdKey, a.Week, a.SalesComponent, a.DuetoValue, a.PrimaryCausalKey, a.CausalValue, a.ModelIteration, a.Published, a.CatLibKey, a.SubBrand, a.BrandName, a.FinancialYear, b.EventDescription, b.Vehicle FROM record2 a LEFT JOIN apps3 b on a.SalesComponent = b.EventList WHERE b.Vehicle is not null"));
		
	    //Query4
		PCollection<BeamRecord> record4 = record3.apply(BeamSql.query(
				"SELECT Vehicle, EventDescription as Campaign, EventDescription as Copy, SalesComponent as Event, CatLib, SubBrand, BrandName, Outlet as Channel, FinancialYear as Period1 from PCOLLECTION GROUP BY Vehicle, EventDescription, SalesComponent, CatLib , SubBrand, BrandName, Outlet, FinancialYear"));

		//Query5
		PCollection<BeamRecord> record5 = record2.apply(BeamSql.query(
				"SELECT MIN(Week1) as StartDate, FinancialYear FROM PCOLLECTION GROUP BY FinancialYear"));

			//Query6
		PCollectionTuple query4 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record4"), record4).and(new TupleTag<BeamRecord>("record5"), record5);	    
		PCollection<BeamRecord> record6 = query4.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1 , b.StartDate as PeriodStartDate FROM record4 a LEFT JOIN record5 b on a.Period1 = b.FinancialYear"));            
	
		//Query7
		PCollection<BeamRecord> record7 = record2.apply(BeamSql.query(" SELECT MAX(Week1) as EndDate, FinancialYear FROM PCOLLECTION GROUP BY FinancialYear"));
		
				//Query8
		PCollectionTuple query5 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record6"), record6).and(new TupleTag<BeamRecord>("record7"), record7);	    
		PCollection<BeamRecord> record8 = query5.apply(BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, b.EndDate as PeriodEndDate from record6 a LEFT JOIN record7 b on a.Period1 = b.FinancialYear"));
        PCollection<BeamRecord> record8_trig = record8.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		//Query9 .... b.Week1 <= a.PeriodEndDate  removed
		PCollectionTuple query6 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record8_trig"), record8_trig).and(new TupleTag<BeamRecord>("record3_trig"), record3_trig);	    
		PCollection<BeamRecord> record9 = query6.apply(BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, SUM(case when b.CausalValue is null then cast('0.00' as double) else b.CausalValue END) as GRPs from record8_trig a LEFT JOIN record3_trig b on a.Channel = b.Outlet and a.SubBrand = b.SubBrand and a.Event = b.SalesComponent and b.Week1 >= a.PeriodStartDate GROUP BY a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate"));
 	     PCollection<BeamRecord> record9_trig= record9.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	
		//Querytest
 				
		//Query10 ... b.Week1 <= a.PeriodEndDate removed and b.CausalValue > 0
		PCollectionTuple query7 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record9_trig"), record9_trig).and(new TupleTag<BeamRecord>("record3_trig"), record3_trig);	    
		PCollection<BeamRecord> record10 = query7.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, MIN(case when b.Week1 is null then 0 else b.Week1 END) as CopyStartWithinPeriod from record9_trig a LEFT JOIN record3_trig b on a.Channel = b.Outlet and a.SubBrand = b.SubBrand and a.Event = b.SalesComponent and b.Week1 >= a.PeriodStartDate WHERE b.Week1 IS NOT NULL group by a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs"));
	    PCollection<BeamRecord> record10_trig= record10.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	
	        	
	 //Query11
		PCollectionTuple query8 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record10_trig"), record10_trig).and(new TupleTag<BeamRecord>("record3_trig"), record3_trig);	    
		PCollection<BeamRecord> record11 = query8.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.CopyStartWithinPeriod, MAX(case when b.Week1 is null then 0 else b.Week1 END) as CopyEndWithinPeriod from record10_trig a LEFT JOIN record3_trig b on a.Channel = b.Outlet and a.SubBrand = b.SubBrand and a.Event = b.SalesComponent and b.Week1 >= a.PeriodStartDate WHERE b.Week1 IS NOT NULL group by a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.CopyStartWithinPeriod"));
	    PCollection<BeamRecord> record11_trig= record11.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

	    
		//Query12
		PCollectionTuple query9 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record11_trig"), record11_trig).and(new TupleTag<BeamRecord>("record3_trig"), record3_trig);	    
		PCollection<BeamRecord> record12 = query9.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.CopyStartWithinPeriod, a.CopyEndWithinPeriod, Count(DISTINCT(case when b.Week1 is null then 0 else b.Week1 END)) as Duration from record11_trig a LEFT JOIN record3_trig b on a.Channel = b.Outlet and a.SubBrand = b.SubBrand and a.Event = b.SalesComponent and (b.Week1 >= a.CopyStartWithinPeriod and b.Week1 <= a.CopyEndWithinPeriod) group by a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.CopyStartWithinPeriod, a.CopyEndWithinPeriod"));
	    PCollection<BeamRecord> record12_trig= record12.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	
	
	  	
	  	    
	//Query13 Case statement of continuity removed
		PCollectionTuple query10 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record12_trig"), record12_trig).and(new TupleTag<BeamRecord>("record3_trig"), record3_trig);	    
		PCollection<BeamRecord> record13 = query10.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.CopyStartWithinPeriod, a.CopyEndWithinPeriod, a.Duration, (COUNT(DISTINCT(case when b.Week1 is null then 0 else b.Week1 END))) as Continuity from record12_trig a LEFT JOIN record3_trig b on a.Channel = b.Outlet and a.SubBrand = b.SubBrand and a.Event = b.SalesComponent and b.Week1 >= a.PeriodStartDate GROUP BY a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.CopyStartWithinPeriod, a.CopyEndWithinPeriod, a.Duration "));
	    PCollection<BeamRecord> record13_trig = record13.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		//Query14
		PCollectionTuple query11 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record8_trig"), record8_trig).and(new TupleTag<BeamRecord>("record3_trig"), record3_trig);	    
		PCollection<BeamRecord> record14 = query11.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, SUM(case when b.DuetoValue IS NULL then cast('0.00' as double) else b.DuetoValue END) as ChannelVolume from record8_trig a LEFT JOIN record3_trig b on a.Channel = b.Outlet and a.BrandName = b.BrandName and a.Event = b.SalesComponent and a.CatLib = b.CatLib and b.Week1 >= a.PeriodStartDate GROUP BY a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate"));
	    PCollection<BeamRecord> record14_trig = record14.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		
		//Query15
		PCollectionTuple query12 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record14_trig"), record14_trig).and(new TupleTag<BeamRecord>("ship_trig"), ship_trig);	    
		PCollection<BeamRecord> record15 = query12.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.ChannelVolume, AVG(case when b.ProjectionFactor is null then cast('0.00' as double) else b.ProjectionFactor END) as ProjectionFactor from record14_trig a LEFT JOIN ship_trig b on a.SubBrand = b.SubBrand and a.CatLib = b.CatLib and a.Period1 = b.Period2 GROUP BY a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.ChannelVolume"));
		
		
		//Query16
		PCollection<BeamRecord> record16 = record15.apply(BeamSql.query(
				" SELECT Vehicle, Campaign, Copy, Event, CatLib, SubBrand, BrandName, Channel, Period1, PeriodStartDate, PeriodEndDate, ChannelVolume, ProjectionFactor, (ChannelVolume)/(ProjectionFactor) as Volume from PCOLLECTION"));
	    PCollection<BeamRecord> record16_trig = record16.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

	    
		//Query17
		PCollectionTuple query13 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record16_trig"), record16_trig).and(new TupleTag<BeamRecord>("finance_trig"), finance_trig);	    
		PCollection<BeamRecord> record17 = query13.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.ChannelVolume, a.ProjectionFactor, a.Volume, SUM(case when b.AdjContribPerSc is null then cast('0.00' as double) else b.AdjContribPerSc END) as AdjContribPerSc from record16_trig a \r\n" + 
				"        LEFT JOIN finance_trig b \r\n" + 
				"        on \r\n" + 
				"        a.BrandName = b.BrandName and \r\n" + 
				"        a.Subbrand = b.SubbrandName and \r\n" + 
				"        a.Period1 = b.Period3 \r\n" + 
				"        Group by a.Vehicle, a.Campaign, a.Copy, a.Event, a.CatLib, a.SubBrand, a.BrandName, a.Channel, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.ChannelVolume, a.ProjectionFactor, a.Volume"));

		
		
		//Query18
		PCollection<BeamRecord> record18 = record17.apply(BeamSql.query(
				" SELECT Vehicle, Campaign, Copy, Event, CatLib, SubBrand, BrandName, Channel, Period1, PeriodStartDate, PeriodEndDate, ChannelVolume, ProjectionFactor, Volume, AdjContribPerSc , (Volume*AdjContribPerSc) as AdjContrib from PCOLLECTION"));
	    PCollection<BeamRecord> record18_trig = record18.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		
		
		//Query19
		PCollection<BeamRecord> record19 = record13.apply(BeamSql.query(
				" SELECT Vehicle, BrandName, Period1, PeriodStartDate, PeriodEndDate from PCOLLECTION GROUP BY Vehicle, BrandName, Period1, PeriodStartDate, PeriodEndDate"));
	    PCollection<BeamRecord> record19_trig = record19.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	
		
		//Query20
		PCollectionTuple query14 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record19_trig"), record19_trig).and(new TupleTag<BeamRecord>("record13_trig"), record13_trig);	    
		PCollection<BeamRecord> record20 = query14.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate , SUM(case when b.GRPs is null then cast('0.00' as double) else b.GRPs END) as GRPs from record19_trig a \r\n" + 
				"        LEFT JOIN record13_trig b on \r\n" + 
				"        a.BrandName = b.BrandName and \r\n" + 
				"        a.Period1 = b.Period1 \r\n" + 
				"        group by a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate"));
	    PCollection<BeamRecord> record20_trig = record20.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		
		//Query21
		PCollectionTuple query15 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record20_trig"), record20_trig).and(new TupleTag<BeamRecord>("record13_trig"), record13_trig);	    
		PCollection<BeamRecord> record21 = query15.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, MAX(case when b.Duration is null then 0 else b.Duration END) as Duration , MAX(case when b.Continuity is null then 0 else b.Continuity END) as Continuity from record20_trig a LEFT JOIN \r\n" + 
				"        record13_trig b on \r\n" + 
				"        a.BrandName = b.BrandName and \r\n" + 
				"        a.Period1 = b.Period1 \r\n" + 
				"        GROUP BY a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs"));

		//Query22_1
		PCollection<BeamRecord> record22_1 = record3.apply(BeamSql.query(
				"  SELECT DISTINCT(FinancialYear) as BasisPeriod from PCOLLECTION"));
	    

	    //temp table for cross join in record22_2
	    PCollection<BeamRecord> recordx1 = record21.apply(BeamSql.query(
				"  SELECT Vehicle, BrandName, Period1, PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, 1 as id1 from PCOLLECTION"));
	    
	    PCollection<BeamRecord> recordx2 = record22_1.apply(BeamSql.query(
				"  SELECT BasisPeriod, 1 as id2 from PCOLLECTION"));
	//Query22_2 
		PCollectionTuple query16 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("recordx1"), recordx1).and(new TupleTag<BeamRecord>("recordx2"), recordx2);	    
		PCollection<BeamRecord> record22_2 = query16.apply(
		BeamSql.queryMulti("SELECT a.*, b.* from recordx1 a INNER JOIN recordx2 b on a.id1 = b.id2"));
	    PCollection<BeamRecord> record22_2_trig = record22_2.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		
	    //Query23
		PCollectionTuple query17 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record22_2_trig"), record22_2_trig).and(new TupleTag<BeamRecord>("spend_trig"), spend_trig);	    
		PCollection<BeamRecord> record23 = query17.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, SUM(case when b.Spend is null then cast('0.00' as double) else b.Spend END) as Basis from \r\n" + 
				"        record22_2_trig a LEFT JOIN \r\n" + 
				"        spend_trig b on \r\n" + 
				"        a.BrandName = b.PrincipalBrand and \r\n" + 
				"        a.Vehicle = b.Vehicle and \r\n" + 
				"        a.BasisPeriod  = b.FiscalYear \r\n" + 
				"        GROUP BY a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod"));
	    PCollection<BeamRecord> record23_trig = record23.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		
		
	 	//Query24
		PCollectionTuple query18 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record23_trig"), record23_trig).and(new TupleTag<BeamRecord>("record13_trig"), record13_trig);	    
		PCollection<BeamRecord> record24 = query18.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis , MAX(case when b.Duration is null then 0 else b.Duration END) as BasisDuration \r\n" + 
				"        from record23_trig a LEFT JOIN \r\n" + 
				"        record13_trig b on \r\n" + 
				"        a.BrandName = b.BrandName and \r\n" + 
				"        a.BasisPeriod = b.Period1 \r\n" + 
				"        group by a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis"));
	    PCollection<BeamRecord> record24_trig = record24.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		
		
		//Query25
		PCollectionTuple query19 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record24_trig"), record24_trig).and(new TupleTag<BeamRecord>("curve_trig"), curve_trig);	    
		PCollection<BeamRecord> record25 = query19.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis, a.BasisDuration, AVG(case when b.Alpha is null then cast('0.00' as double) else b.Alpha END) as Alpha, AVG(case when b.Beta is null then cast('0.00' as double) else b.Beta END) as Beta \r\n" + 
				"        from record24_trig a LEFT JOIN \r\n" + 
				"        curve_trig b on\r\n" + 
				"        a.BrandName = b.Brand and \r\n" + 
				"        a.Vehicle = b.Vehicle \r\n" + 
				"        GROUP BY a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis, a.BasisDuration"));
	    PCollection<BeamRecord> record25_trig = record25.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		//Query26
		PCollectionTuple query20 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record25_trig"), record25_trig).and(new TupleTag<BeamRecord>("record18_trig"), record18_trig);	    
		PCollection<BeamRecord> record26 = query20.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis, a.BasisDuration, a.Alpha, a.Beta , SUM(case when b.Volume is null then cast('0.00' as double) else b.Volume END) as Volume  from record25_trig a LEFT JOIN \r\n" + 
				"        record18_trig b on \r\n" + 
				"        a.BrandName = b.BrandName and \r\n" + 
				"        a.Vehicle = b.Vehicle and \r\n" + 
				"        a.Period1 = b.Period1 \r\n" + 
				"        group by a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis, a.BasisDuration, a.Alpha, a.Beta"));
	    PCollection<BeamRecord> record26_trig = record26.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		
		//Query27
		PCollectionTuple query21 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record26_trig"), record26_trig).and(new TupleTag<BeamRecord>("spend_trig"), spend_trig);	    
		PCollection<BeamRecord> record27 = query21.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis, a.BasisDuration, a.Alpha, a.Beta , a.Volume , SUM(case when b.Spend is null then cast('0.00' as double) else b.Spend END) as Spend from record26_trig a LEFT JOIN \r\n" + 
				"        spend_trig b on \r\n" + 
				"        a.BrandName = b.PrincipalBrand and \r\n" + 
				"        a.Period1 = b.FiscalYear \r\n" + 
				"        group by a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis, a.BasisDuration, a.Alpha, a.Beta , a.Volume "));
		

		//Query28_1 Effieciency not done properly
		PCollection<BeamRecord> record28_1 = record27.apply(BeamSql.query(
				" SELECT Vehicle, BrandName, Period1, PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, BasisPeriod, Basis, BasisDuration, Alpha, Beta, Volume, Spend, Volume*10 as Efficiency from PCOLLECTION"));
		
		//Query28_2
		PCollection<BeamRecord> record28_2 = record28_1.apply(BeamSql.query(
				" SELECT Vehicle, BrandName, Period1, PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, BasisPeriod, Basis, BasisDuration, Alpha, Beta, Volume, Spend, Efficiency, (Vehicle||'_'||Period1||'_'||BasisPeriod) as Key from PCOLLECTION"));
		

				
		//Query28_3
		PCollectionTuple query22 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record28_2"), record28_2).and(new TupleTag<BeamRecord>("xnorm_trig"), xnorm_trig);	    
		PCollection<BeamRecord> record28_3 = query22.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis, a.BasisDuration, a.Alpha, a.Beta, a.Volume, a.Spend, a.Efficiency, a.Key, (case when b.Xnorm is null then cast('0.0' as double) else b.Xnorm END) as Xnorm from record28_2 a LEFT JOIN \r\n" + 
				"        xnorm_trig b on \r\n" + 
				"        a.Key = b.Key"));
		
		//Query28_4
		PCollection<BeamRecord> record28_4 = record28_3.apply(BeamSql.query(
				"  SELECT Vehicle, BrandName, Period1, PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, BasisPeriod, Basis, BasisDuration, Alpha, Beta, Volume, Spend, Efficiency, Key, Xnorm, (Volume*10.2) as NormalizedEfficiency from PCOLLECTION"));
				
		
		//Query28_5
		PCollectionTuple query23 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record28_4"), record28_4).and(new TupleTag<BeamRecord>("record18_trig"), record18_trig);	    
		PCollection<BeamRecord> record28_5 = query23.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis, a.BasisDuration, a.Alpha, a.Beta, a.Volume, a.Spend, a.Efficiency, a.Key, a.Xnorm, a.NormalizedEfficiency , SUM(case when b.AdjContrib is null then cast('0.00' as double) else b.AdjContrib END) as AdjContrib from record28_4  a \r\n" + 
				"        LEFT JOIN record18_trig b on \r\n" + 
				"        a.BrandName = b.BrandName and \r\n" + 
				"        a.Period1 = b.Period1 \r\n" + 
				"        group by a.Vehicle, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.BasisPeriod, a.Basis, a.BasisDuration, a.Alpha, a.Beta, a.Volume, a.Spend, a.Efficiency, a.Key, a.Xnorm, a.NormalizedEfficiency"));

		//Query28_6
		PCollection<BeamRecord> record28_6 = record28_5.apply(BeamSql.query(
				"  SELECT  Vehicle, BrandName, Period1, PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, BasisPeriod, Basis, BasisDuration, Alpha, Beta, Volume, Spend, Efficiency, Key, Xnorm, NormalizedEfficiency, AdjContrib, AdjContrib*10 as Payout , NormalizedEfficiency*Basis as NormalizedVolume, \r\n" + 
				"        (NormalizedEfficiency*Basis)*(AdjContrib) as NormalizedAdjContrib , \r\n" + 
				"        ((NormalizedEfficiency*Basis)*(AdjContrib)) as NormalizedPayout \r\n" + 
				"        from PCOLLECTION"));

		//Query33_1 Vechileeff
		PCollection<BeamRecord> record33_1 = record28_6.apply(BeamSql.query(
				"SELECT Vehicle	,BrandName,	Period1	,PeriodStartDate ,PeriodEndDate, GRPs,	Duration	,Continuity,	BasisPeriod,	Basis,	BasisDuration,	Alpha,	\r\n" + 
				"        Beta,	Volume,	Spend, Efficiency, Xnorm, NormalizedEfficiency,	AdjContrib,Payout ,NormalizedVolume,NormalizedAdjContrib, NormalizedPayout \r\n" + 
				"        from PCOLLECTION"));
		
		
		//Query29
		PCollection<BeamRecord> record29 = record13.apply(BeamSql.query("SELECT Vehicle ,Campaign , Copy, BrandName,Period1,PeriodStartDate,PeriodEndDate from PCOLLECTION \r\n" + 
				"        where Copy <> '#N/A' \r\n" + 
				"        group by Vehicle, BrandName,Period1,PeriodStartDate,PeriodEndDate, Campaign, Copy"));
	    PCollection<BeamRecord> record29_trig = record29.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

	
		//Query30
		PCollectionTuple query24 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record29_trig"), record29_trig).and(new TupleTag<BeamRecord>("record13_trig"), record13_trig);	    
		PCollection<BeamRecord> record30 = query24.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle,a.Campaign , a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate , SUM(case when b.GRPs is null then cast('0.00' as double) else b.GRPs END) as GRPs , MAX(case when b.Duration is null then cast('0.00' as double) else b.Duration END) as Duration , SUM(case when b.Continuity is null then cast('0.00' as double) else b.Continuity END) as Continuity from record29_trig a LEFT JOIN \r\n" + 
				"        record13_trig b on \r\n" + 
				"        a.Copy = b.Copy  and \r\n" + 
				"        a.Period1 = b.Period1 \r\n" + 
				"        group by a.Vehicle,a.Campaign , a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate"));
	    PCollection<BeamRecord> record30_trig = record30.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

				
		//Query31
		PCollectionTuple query25 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record30_trig"), record30_trig).and(new TupleTag<BeamRecord>("basis_trig"), basis_trig);	    
		PCollection<BeamRecord> record31 = query25.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle,a.Campaign , a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, SUM(case when b.Basis is null then cast('0.00' as double) else b.Basis END) as Basis , SUM(case when b.BasisDuration is null then cast('0.00' as double) else b.BasisDuration END) as BasisDuration from record30_trig a LEFT JOIN \r\n" + 
				"        basis_trig b on \r\n" + 
				"        a.Vehicle = b.Vehicle \r\n" + 
				"        group by a.Vehicle,a.Campaign, a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity")); 
	    PCollection<BeamRecord> record31_trig = record31.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	
		
		//Query32
		PCollectionTuple query26 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record31_trig"), record31_trig).and(new TupleTag<BeamRecord>("curve_trig"), curve_trig);	    
		PCollection<BeamRecord> record32 = query26.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle,a.Campaign , a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.Basis, a.BasisDuration, AVG((case when b.Alpha is null then cast('0.00' as double) else b.Alpha END)) as Alpha , AVG((case when b.Beta is null then cast('0.00' as double) else b.Beta END)) as Beta  from record31_trig a LEFT JOIN \r\n" + 
				"        curve_trig b on\r\n" + 
				"        a.Vehicle = b.Vehicle and \r\n" + 
				"        a.BrandName = b.Brand \r\n" + 
				"        group by a.Vehicle,a.Campaign , a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.Basis, a.BasisDuration"));
	    PCollection<BeamRecord> record32_trig = record32.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

				
		//Query33
		PCollectionTuple query27 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record32_trig"), record32_trig).and(new TupleTag<BeamRecord>("record18_trig"), record18_trig);	    
		PCollection<BeamRecord> record33 = query27.apply(
		BeamSql.queryMulti("SELECT a.Vehicle,a.Campaign , a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.Alpha, a.Beta,a.Basis, a.BasisDuration, SUM(case when b.Volume is null then cast('0.00' as double) else b.Volume END) as Volume from record32_trig a LEFT JOIN \r\n" + 
				"        record18_trig b on \r\n" + 
				"\r\n" + 
				"        a.Copy = b.Copy and \r\n" + 
				"        a.Period1 = b.Period1 \r\n" + 
				"        group by a.Vehicle,a.Campaign , a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.Alpha, a.Beta,a.Basis, a.BasisDuration"));
	    PCollection<BeamRecord> record33_trig = record33.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	


		//Query34
		PCollectionTuple query28 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record33_trig"), record33_trig).and(new TupleTag<BeamRecord>("spend_trig"), spend_trig);	    
		PCollection<BeamRecord> record34 = query28.apply(
		BeamSql.queryMulti("SELECT a.Vehicle,a.Campaign , a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.Alpha, a.Beta, a.Volume,a.Basis, a.BasisDuration , SUM(case when b.Spend is null then cast('0.00' as double) else b.Spend END) as Spend from record33_trig a LEFT JOIN \r\n" + 
				"        spend_trig b on\r\n" + 
				"\r\n" + 
				"        a.Copy = b.Copy and \r\n" + 
				"        a.Period1 = b.FiscalYear\r\n" + 
				"        group by a.Vehicle,a.Campaign , a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.Alpha, a.Beta, a.Volume, a.Basis, a.BasisDuration"));
		
		//Query35
		PCollection<BeamRecord> record35 = record34.apply(BeamSql.query("SELECT Vehicle, Campaign, Copy, BrandName, Period1, PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, Alpha, Beta,Basis, BasisDuration, Volume, Spend, Basis, BasisDuration, (Volume*10) as Efficiency from PCOLLECTION"));

		//Query36
		PCollection<BeamRecord> record36 = record35.apply(BeamSql.query("SELECT Vehicle, Campaign, Copy, BrandName, Period1, PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, Alpha, Beta, Basis, BasisDuration, Volume, Spend, Efficiency, (Copy||'_'||Period1) as Key from PCOLLECTION"));
	    PCollection<BeamRecord> record36_trig = record36.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		//Query37
		PCollectionTuple query29 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record36_trig"), record36_trig).and(new TupleTag<BeamRecord>("xcam_trig"), xcam_trig);	    
		PCollection<BeamRecord> record37 = query29.apply(
		BeamSql.queryMulti(" SELECT a.Vehicle, a.Campaign, a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.Alpha, a.Beta,a.Basis, a.BasisDuration, a.Volume, a.Spend, a.Efficiency, a.Key, (case when b.Xnorm is null then cast('0.0' as double) else b.Xnorm END) as Xnorm from record36_trig a \r\n" + 
				"        LEFT JOIN xcam_trig b \r\n" + 
				"        on a.Key = b.Key"));
				
		
	//Query38
		PCollection<BeamRecord> record38 = record37.apply(BeamSql.query(" SELECT Vehicle, Campaign, Copy, BrandName, Period1, PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, Alpha, Beta, Basis, BasisDuration,Volume, Spend, Efficiency, Key, Xnorm, (Volume*Spend) as NormalizedEfficiency from PCOLLECTION"));
		PCollection<BeamRecord> record38_trig = record38.apply(Window.<BeamRecord>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow())).withAllowedLateness(org.joda.time.Duration.standardMinutes(1)).discardingFiredPanes());	

		//Query39
		PCollectionTuple query30 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record38_trig"), record38_trig).and(new TupleTag<BeamRecord>("record18_trig"), record18_trig);	    
		PCollection<BeamRecord> record39 = query30.apply(
		BeamSql.queryMulti("SELECT a.Vehicle, a.Campaign, a.Copy, a.BrandName, a.Period1,a.PeriodStartDate,a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.Alpha, a.Beta, a.Basis, a.BasisDuration, a.Volume, a.Spend, a.Efficiency, a.Key, a.Xnorm, a.NormalizedEfficiency, SUM(case when b.AdjContrib is null then cast('0.00' as double) else b.AdjContrib END) as AdjContrib from record38_trig a \r\n" + 
				"        LEFT JOIN record18_trig b on \r\n" + 
				"        a.copy = b.copy and \r\n" + 
				"        a.period1 = b.period1 \r\n" + 
				"        group by a.Vehicle, a.Campaign, a.Copy, a.BrandName, a.Period1, a.PeriodStartDate, a.PeriodEndDate, a.GRPs, a.Duration, a.Continuity, a.Alpha, a.Beta, a.Volume, a.Spend, a.Efficiency, a.Key, a.Xnorm, a.NormalizedEfficiency, a.Basis, a.BasisDuration"));

		PCollection<BeamRecord> record40_1 = record39.apply(
				BeamSql.query("SELECT Vehicle, Campaign, Copy, BrandName, Period1, PeriodStartDate, PeriodEndDate , GRPs, Duration, Continuity, Alpha, Beta, Basis, BasisDuration, Volume, Spend, Efficiency, Key, Xnorm, NormalizedEfficiency, AdjContrib, (AdjContrib*10) as Payout , (NormalizedEfficiency*5) as NormalizedVolume, (NormalizedEfficiency*4) as NormalizedAdjContrib , (NormalizedEfficiency)*8 as NormalizedPayout \r\n" + 
						"from PCOLLECTION "));
		
		PCollection<BeamRecord> record40_2 = record39.apply(
				BeamSql.query("SELECT Vehicle, Campaign, Copy, BrandName, Period1, PeriodStartDate, PeriodEndDate, GRPs, Duration, Continuity, Alpha, Beta, Basis, BasisDuration, Volume, Spend, Efficiency, Key, Xnorm, NormalizedEfficiency, AdjContrib, AdjContrib as Payout, NormalizedEfficiency as NormalizedVolume, NormalizedEfficiency as NormalizedAdjContrib, (NormalizedEfficiency*2) as NormalizedPayout from PCOLLECTION"));

		//Query40
		PCollectionTuple query31 = PCollectionTuple.of(
			    new TupleTag<BeamRecord>("record40_1"), record40_1).and(new TupleTag<BeamRecord>("record40_2"), record40_2);	    
		PCollection<BeamRecord> record40 = query31.apply(
		BeamSql.queryMulti(" SELECT * from record40_1 UNION ALL SELECT * from record40_2"));

	/*	   //convert BeamRecord into String to get output in GS
		   PCollection<String> gs_output_final = record40.apply(ParDo.of(new DoFn<BeamRecord, String>() {
				private static final long serialVersionUID = 1L;
				@ProcessElement
				public void processElement(ProcessContext c) {
					c.output(c.element().toString());
					System.out.println(c.element().toString());
				}
			}));
					gs_output_final.apply(TextIO.write().to("gs://cloroxtegbucket/output/Q40test111"));*/
		 
		// Writing to BigQuery starts from here 
		// convert BeamRecord into class typ
	  PCollection<MyOutputClass> final_out = record40.apply(ParDo.of(new DoFn<BeamRecord, MyOutputClass>() {
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext c) throws ParseException {
				 BeamRecord record = c.element();
		           String strArr = record.toString();
		           String strArr1 = strArr.substring(24);
		           String xyz = strArr1.replace("]","");
		           String[] strArr2 = xyz.split(",");
		       	MyOutputClass moc = new MyOutputClass();
				moc.setVehicle(strArr2[0]);
				moc.setCampaign(strArr2[1]);
				moc.setCopy(strArr2[2]);
				moc.setBrandName(strArr2[3]);
				moc.setPeriod1(strArr2[4]);
				moc.setPeriodStartDate(strArr2[5]);
				moc.setPeriodEndDate(strArr2[6]);
				moc.setGRPs(Float.parseFloat(strArr2[7]));
				moc.setDuration(Float.parseFloat(strArr2[8]));
				moc.setContinuity(Float.parseFloat(strArr2[9]));
				moc.setAlpha(Float.parseFloat(strArr2[10]));
				moc.setBeta(Float.parseFloat(strArr2[11]));
				moc.setBasis(Float.parseFloat(strArr2[12]));
				moc.setBasisDuration(Float.parseFloat(strArr2[13]));
				moc.setVolume(Float.parseFloat(strArr2[14]));
				moc.setSpend(Float.parseFloat(strArr2[15]));
				moc.setEfficiency(Float.parseFloat(strArr2[16]));
				moc.setKey(strArr2[17]);
				moc.setXnorm(Float.parseFloat(strArr2[18]));
				moc.setNormalizedEfficiency(Float.parseFloat(strArr2[19]));
				moc.setAdjContrib(Float.parseFloat(strArr2[20]));
				moc.setPayout(Float.parseFloat(strArr2[21]));
				moc.setNormalizedVolume(Float.parseFloat(strArr2[22]));
				moc.setNormalizedAdjContrib(Float.parseFloat(strArr2[23]));
				moc.setNormalizedPayout(Float.parseFloat(strArr2[24]));
				c.output(moc);
			}
		}));
	  
	/*  //convert string into MyOutputClass
	  PCollection<MyOutputClass> final_out = str_join.apply(ParDo.of(new DoFn<String, MyOutputClass>() {
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void processElement(ProcessContext c) {
				String[] strArr = c.element().toString().split(",");
				System.out.println(strArr);
				MyOutputClass moc = new MyOutputClass();
				moc.setSalesComponent(strArr[0]);
				moc.setDuetoValue(strArr[1]);
				moc.setModelIteration(strArr[2]);
				c.output(moc);
			}
		}));*/
	  
	  TableSchema tableSchema = new TableSchema().setFields(ImmutableList.of(
	          new TableFieldSchema().setName("Vehicle").setType("STRING").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Campaign").setType("STRING").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Copy").setType("STRING").setMode("NULLABLE"),
	          new TableFieldSchema().setName("BrandName").setType("STRING").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Period1").setType("STRING").setMode("NULLABLE"),
	          new TableFieldSchema().setName("PeriodStartDate").setType("STRING").setMode("NULLABLE"),
	          new TableFieldSchema().setName("PeriodEndDate").setType("STRING").setMode("NULLABLE"),
	          new TableFieldSchema().setName("GRPs").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Duration").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Continuity").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Alpha").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Beta").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Basis").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("BasisDuration").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Volume").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Spend").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Efficiency").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Key").setType("STRING").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Xnorm").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("NormalizedEfficiency").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("AdjContrib").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("Payout").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("NormalizedVolume").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("NormalizedAdjContrib").setType("FLOAT64").setMode("NULLABLE"),
	          new TableFieldSchema().setName("NormalizedPayout").setType("FLOAT64").setMode("NULLABLE")));
		
	     TableReference tableSpec = BigQueryHelpers.parseTableSpec("beta-194409:data_id1.cloroxtest");
	  System.out.println("Start Bigquery");
	  
	  final_out.apply(MapElements.into(TypeDescriptor.of(TableRow.class)).via(
              (MyOutputClass elem) -> new TableRow().set("Vehicle", elem.Vehicle).set("Campaign", elem.Campaign).set("Copy", elem.Copy).set("BrandName", elem.BrandName).set("Period1", elem.Period1).set("PeriodStartDate", elem.PeriodStartDate).set("PeriodEndDate", elem.PeriodEndDate).set("GRPs", elem.GRPs).set("Duration", elem.Duration).set("Continuity", elem.Continuity).set("Alpha", elem.Alpha).set("Beta", elem.Beta)
              .set("Basis", elem.Basis).set("BasisDuration", elem.BasisDuration).set("Volume", elem.Volume).set("Spend", elem.Spend).set("Efficiency", elem.Efficiency).set("Key", elem.Key).set("Xnorm", elem.Xnorm).set("NormalizedEfficiency", elem.NormalizedEfficiency).set("AdjContrib", elem.AdjContrib)
              .set("Payout", elem.Payout).set("NormalizedVolume", elem.NormalizedVolume).set("NormalizedAdjContrib", elem.NormalizedAdjContrib).set("NormalizedPayout", elem.NormalizedPayout)))
	  			  .apply(BigQueryIO.writeTableRows()
                  .to(tableSpec)
                  .withSchema(tableSchema)
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                  .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
	  
		p.run().waitUntilFinish();
		}
}