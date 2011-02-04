//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop.pagerank;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import edu.csueb.vhendrix.hadoop.pagerank.PageRank;

public class PageRankTest
{

    private static final String PART_R_00000 = "part-r-00000";

    @Test
    public void testPageRank() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        Path input = new Path("target/test-classes/data/pagerank/in/page-rank-graph.txt");
        Path output = new Path("target/test-classes/data/pagerank/out");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output

        PageRank driver = new PageRank();
        driver.setConf(conf);

        int exitCode = driver.run(new String[]
        { input.toString(), output.toString() });
        assertThat(exitCode, is(0));
        Map<String, String> check = new HashMap<String, String>();
        check.put("1", "0.20730903702184947,{3 4 5}");
        check.put("10", "0.1818385738406097,{3 6 9}");
        check.put("2", "0.1741236289674374,{5 6 7}");
        check.put("3", "0.2698505964557242,{10 1}");
        check.put("4", "0.23800714589050154,{1 2 3 4}");
        check.put("5", "0.23775834653560818,{7 1 2 3}");
        check.put("6", "0.19220547380818434,{5 1}");
        check.put("7", "0.18163513825588454,{3 4 10}");
        check.put("8", "0.15000000000000002,{1 4 7 10}");
        check.put("9", "0.1805564964089702,{3 4 5 9}");

        checkOutput(conf, new Path(output.toString()+"15"), check);
    }
    
    @Test
    public void testPageRankEnwiki() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        Path input = new Path("target/test-classes/data/pagerank/in/enwiki-graph.txt");
        Path output = new Path("target/test-classes/data/pagerank/enwiki/out");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output

        PageRank driver = new PageRank();
        driver.setConf(conf);

        int exitCode = driver.run(new String[]
        { input.toString(), output.toString() });
        assertThat(exitCode, is(0));
        List<String> check = new ArrayList<String>();
        check.add("Computeraccessibility\t0.2798305551805329");  
        check.add("19thcenturyphilosophy\t0.15233055518053293");
        check.add("A.S.Neill\t0.15233055518053293");
        check.add("AdolpheThiers\t0.15233055518053293");
        check.add("AgeofEnlightenment\t0.15233055518053293");
        check.add("AlexComfort\t0.15233055518053293");
        check.add("AlexanderBerkman\t0.15233055518053293");
        check.add("AlexandreSkirda\t0.15233055518053293");
        check.add("Anarcha-feminism\t0.15233055518053293");
        check.add("AnarchistSt.ImierInternational\t0.15233055518053293");
        check.add("Anarchistsymbolism\t0.15233055518053293");
        check.add("ArthurRimbaud\t0.15233055518053293");
        check.add("Barcelona\t0.15233055518053293");
        check.add("BenedictAnderson\t0.15233055518053293");
        check.add("BenoîtBroutchoux\t0.15233055518053293");
        check.add("Bolshevik\t0.15233055518053293");
        check.add("Catalanpeople\t0.15233055518053293");
        check.add("ChristiaanCornelissen\t0.15233055518053293");
        check.add("CliffordHarper\t0.15233055518053293");
        check.add("Comintern\t0.15233055518053293");
        check.add("Commune(socialism)\t0.15233055518053293");
        check.add("Communistparty\t0.15233055518053293");
        check.add("ConfederaciónNacionaldelTrabajo\t0.15233055518053293");
        check.add("Confédérationgénéraledutravail\t0.15233055518053293");
        check.add("Crypto-anarchism\t0.15233055518053293");
        check.add("DanielGuerin\t0.15233055518053293");
        check.add("DieloTruda\t0.15233055518053293");
        check.add("DoraMarsden\t0.15233055518053293");
        check.add("DwightMacdonald\t0.15233055518053293");
        check.add("EmileArmand\t0.15233055518053293");
        check.add("EmmaGoldman\t0.15233055518053293");
        check.add("ErricoMalatesta\t0.15233055518053293");
        check.add("Europeanindividualistanarchism\t0.15233055518053293");
        check.add("FebruaryRevolution\t0.15233055518053293");
        check.add("FrancescFerreriGuàrdia\t0.15233055518053293");
        check.add("FranciscoFranco\t0.15233055518053293");
        check.add("Freelove\t0.15233055518053293");
        check.add("Freethought\t0.15233055518053293");
        check.add("FriedrichEngels\t0.15233055518053293");
        check.add("GeorgeWoodcock\t0.15233055518053293");
        check.add("Greeklanguage\t0.15233055518053293");
        check.add("Greenanarchism\t0.15233055518053293");
        check.add("GustaveCourbet\t0.15233055518053293");
        check.add("HagueCongress(1872)\t0.15233055518053293");
        check.add("IndustrialWorkersoftheWorld\t0.15233055518053293");
        check.add("InternationalAnarchistCongressofAmsterdam\t0.15233055518053293");
        check.add("InternationalWorkers'==Anarchistschoolsofthought==\t0.15233055518053293");
        check.add("InternationalofAnarchistFederations\t0.15233055518053293");
        check.add("IsaacPuente\t0.15233055518053293");
        check.add("IvanIllich\t0.15233055518053293");
        check.add("JamesGuillaume\t0.15233055518053293");
        check.add("Jean-JacquesRousseau\t0.15233055518053293");
        check.add("JohannMost\t0.15233055518053293");
        check.add("JohnHenryMackay\t0.15233055518053293");
        check.add("JohnZerzan\t0.15233055518053293");
        check.add("LGBT\t0.15233055518053293");
        check.add("Laozi\t0.15233055518053293");
        check.add("LawrenceJarach\t0.15233055518053293");
        check.add("LeagueofPeaceandFreedom\t0.15233055518053293");
        check.add("Listofanarchistcommunities\t0.15233055518053293");
        check.add("Listofanarchistmovementsbyregion\t0.15233055518053293");
        check.add("Listsofanarchismtopics\t0.15233055518053293");
        check.add("LucyParsons\t0.15233055518053293");
        check.add("LuigiFabbri\t0.15233055518053293");
        check.add("ManchesterUniversityPress\t0.15233055518053293");
        check.add("Maquis(WorldWarII)\t0.15233055518053293");
        check.add("Marxist\t0.15233055518053293");
        check.add("MaxNettlau\t0.15233055518053293");
        check.add("MaxStirner\t0.15233055518053293");
        check.add("MichelOnfray\t0.15233055518053293");
        check.add("MikhailBakunin\t0.15233055518053293");
        check.add("Mutualism(economictheory)\t0.15233055518053293");
        check.add("NapoleonIIIofFrance\t0.15233055518053293");
        check.add("Nazism\t0.15233055518053293");
        check.add("NestorMakhno\t0.15233055518053293");
        check.add("NewLeftReview\t0.15233055518053293");
        check.add("Nihilistmovement\t0.15233055518053293");
        check.add("OctoberRevolution\t0.15233055518053293");
        check.add("OrganizationalPlatformoftheGeneralUnionofAnarchists(Draft)\t0.15233055518053293");
        check.add("ParisCommune\t0.15233055518053293");
        check.add("PeterKropotkin\t0.15233055518053293");
        check.add("PeterMarshall(author)\t0.15233055518053293");
        check.add("Pierre-JosephProudhon\t0.15233055518053293");
        check.add("PierreMonatte\t0.15233055518053293");
        check.add("Pissarro\t0.15233055518053293");
        check.add("Politicalrepression\t0.15233055518053293");
        check.add("Post-anarchism\t0.15233055518053293");
        check.add("Progressiveeducation\t0.15233055518053293");
        check.add("Reciprocity(culturalanthropology)\t0.15233055518053293");
        check.add("ReignofTerror\t0.15233055518053293");
        check.add("RepublicanParty(UnitedStates)\t0.15233055518053293");
        check.add("RobertOwen\t0.15233055518053293");
        check.add("Robespierre\t0.15233055518053293");
        check.add("RoutledgeEncyclopediaofPhilosophy\t0.15233055518053293");
        check.add("RudolfRocker\t0.15233055518053293");
        check.add("RussianRevolution(1917)\t0.15233055518053293");
        check.add("SituationistInternational\t0.15233055518053293");
        check.add("Sovereignstate\t0.15233055518053293");
        check.add("SpanishCivilWar\t0.15233055518053293");
        check.add("SummerhillSchool\t0.15233055518053293");
        check.add("Syncreticpolitics\t0.15233055518053293");
        check.add("Taoism\t0.15233055518053293");
        check.add("TheFalsePrincipleofourEducation\t0.15233055518053293");
        check.add("TheGlobeandMail\t0.15233055518053293");
        check.add("TheJoyofSex\t0.15233055518053293");
        check.add("VoltairinedeCleyre\t0.15233055518053293");
        check.add("W.W.Norton&amp;Company\t0.15233055518053293");
        check.add("WilliamBatchelderGreene\t0.15233055518053293");
        check.add("WilliamGodwin\t0.15233055518053293");
        check.add("WilliamMcKinley\t0.15233055518053293");
        check.add("WorldWarI\t0.15233055518053293");
        check.add("anarchismandcapitalism\t0.15233055518053293");
        check.add("anarchismandviolence\t0.15233055518053293");
        check.add("anarchist\t0.15233055518053293");
        check.add("anarchistcommunism\t0.15233055518053293");
        check.add("anarchistcommunists\t0.15233055518053293");
        check.add("anarchistfreeschool\t0.15233055518053293");
        check.add("anarchistschoolsofthought\t0.15233055518053293");
        check.add("anarcho-communism\t0.15233055518053293");
        check.add("anarcho-pacifist\t0.15233055518053293");
        check.add("anarcho-primitivism\t0.15233055518053293");
        check.add("anarcho-syndicalism\t0.15233055518053293");
        check.add("anarchy\t0.15233055518053293");
        check.add("anti-globalisationmovement\t0.15233055518053293");
        check.add("antimilitarism\t0.15233055518053293");
        check.add("autonomistmarxism\t0.15233055518053293");
        check.add("civilization\t0.15233055518053293");
        check.add("coercion\t0.15233055518053293");
        check.add("collectivistanarchism\t0.15233055518053293");
        check.add("communards\t0.15233055518053293");
        check.add("counter-economics\t0.15233055518053293");
        check.add("counterculture\t0.15233055518053293");
        check.add("democracy\t0.15233055518053293");
        check.add("deschooling\t0.15233055518053293");
        check.add("dictatorshipoftheproletariat\t0.15233055518053293");
        check.add("directaction\t0.15233055518053293");
        check.add("fascism\t0.15233055518053293");
        check.add("federation\t0.15233055518053293");
        check.add("genderrole\t0.15233055518053293");
        check.add("generalstrike\t0.15233055518053293");
        check.add("hedonist\t0.15233055518053293");
        check.add("ideology\t0.15233055518053293");
        check.add("individual\t0.15233055518053293");
        check.add("individualistanarchism\t0.15233055518053293");
        check.add("individualistanarchist\t0.15233055518053293");
        check.add("insurrectionaryanarchism\t0.15233055518053293");
        check.add("internationalterrorism\t0.15233055518053293");
        check.add("labormovement\t0.15233055518053293");
        check.add("left-wingpolitics\t0.15233055518053293");
        check.add("loisscélérates\t0.15233055518053293");
        check.add("main\t0.15233055518053293");
        check.add("meansofproduction\t0.15233055518053293");
        check.add("nonviolence\t0.15233055518053293");
        check.add("patriarchy\t0.15233055518053293");
        check.add("penalcolonies\t0.15233055518053293");
        check.add("philosophicalanarchism\t0.15233055518053293");
        check.add("philosophy\t0.15233055518053293");
        check.add("politicalphilosophy\t0.15233055518053293");
        check.add("populareducation\t0.15233055518053293");
        check.add("post-leftanarchy\t0.15233055518053293");
        check.add("post-modernism\t0.15233055518053293");
        check.add("postcolonialism\t0.15233055518053293");
        check.add("poststructuralist\t0.15233055518053293");
        check.add("propagandaofthedeed\t0.15233055518053293");
        check.add("radicalfeminism\t0.15233055518053293");
        check.add("revolutionsof1848\t0.15233055518053293");
        check.add("self-governance\t0.15233055518053293");
        check.add("sexualrevolution\t0.15233055518053293");
        check.add("socialistmovement\t0.15233055518053293");
        check.add("socialmovement\t0.15233055518053293");
        check.add("socialorganisation\t0.15233055518053293");
        check.add("socialrevolution\t0.15233055518053293");
        check.add("squatting\t0.15233055518053293");
        check.add("syndicalism\t0.15233055518053293");
        check.add("technology\t0.15233055518053293");
        check.add("tradeunion\t0.15233055518053293");
        check.add("unschooling\t0.15233055518053293");
        check.add("will(philosophy)\t0.15233055518053293");
        check.add("HistoryofAfghanistan\t0.15228795578193619");
        check.add("GeographyofAfghanistan\t0.15205271592953767");
        check.add("DemographyofAfghanistan\t0.15170147626011618");
        check.add("CommunicationsinAfghanistan\t0.15156090294148333");
        check.add("TransportinAfghanistan\t0.15086418163000792");
        check.add("MilitaryofAfghanistan\t0.15076418163000793");
        check.add("ForeignrelationsofAfghanistan\t0.15058856179529717");
        check.add("Assistive_technology\t0.15047180355353892");
        check.add("Amoeboid\t0.1504348363166587"); 
        check.add("ADHD\t0.15038954324383452");
        check.add("Alternativetherapiesfordevelopmentalandlearningdisabilities\t0.15038954324383452");
        check.add("Apraxia\t0.15038954324383452");
        check.add("Aspergersyndrome\t0.15038954324383452");
        check.add("AutismDiagnosticInterview-Revised\t0.15038954324383452");
        check.add("AutismDiagnosticObservationSchedule\t0.15038954324383452");
        check.add("ChildhoodAutismRatingScale\t0.15038954324383452");
        check.add("Clinicalgenetics\t0.15038954324383452");
        check.add("EugenBleuler\t0.15038954324383452");
        check.add("Executivedysfunction\t0.15038954324383452");
        check.add("Gesture\t0.15038954324383452");
        check.add("HansAsperger\t0.15038954324383452");
        check.add("Heritabilityofautism\t0.15038954324383452");
        check.add("Humanembryogenesis\t0.15038954324383452");
        check.add("InhibitionTheory\t0.15038954324383452");
        check.add("Interpersonalrelationship\t0.15038954324383452");
        check.add("LeoKanner\t0.15038954324383452");
        check.add("MartinLuther\t0.15038954324383452");
        check.add("Medicaldiagnosis\t0.15038954324383452");
        check.add("Mendelian\t0.15038954324383452");
        check.add("Neurodevelopmentaldisorder\t0.15038954324383452");
        check.add("NewLatin\t0.15038954324383452");
        check.add("PDDnototherwisespecified\t0.15038954324383452");
        check.add("Parkinson'smechanismincludesalterationofbraindevelopmentsoonafterconception.&lt;refname=Arndt*Anexcessof[[neuron\t0.15038954324383452");
        check.add("PaulOffit\t0.15038954324383452");
        check.add("Rettsyndrome\t0.15038954324383452");
        check.add("Savantsyndrome\t0.15038954324383452");
        check.add("Screening(medicine)\t0.15038954324383452");
        check.add("Sensorysystem\t0.15038954324383452");
        check.add("Spectrumdisorder\t0.15038954324383452");
        check.add("Swiss\t0.15038954324383452");
        check.add("Synapse\t0.15038954324383452");
        check.add("Tabletalk(literature)\t0.15038954324383452");
        check.add("Tourettesyndrome\t0.15038954324383452");
        check.add("ViennaGeneralHospital\t0.15038954324383452");
        check.add("Weakcentralcoherencetheory\t0.15038954324383452");
        check.add("amygdala\t0.15038954324383452");
        check.add("autismspectrumdisorder\t0.15038954324383452");
        check.add("babbling\t0.15038954324383452");
        check.add("cerebellum\t0.15038954324383452");
        check.add("childhooddisintegrativedisorder\t0.15038954324383452");
        check.add("chromosomeabnormalities\t0.15038954324383452");
        check.add("chromosomeabnormality\t0.15038954324383452");
        check.add("cognitive\t0.15038954324383452");
        check.add("communication\t0.15038954324383452");
        check.add("defaultnetwork\t0.15038954324383452");
        check.add("dendriticspine\t0.15038954324383452");
        check.add("echolalia\t0.15038954324383452");
        check.add("empathizing–systemizingtheory\t0.15038954324383452");
        check.add("event-relatedpotential\t0.15038954324383452");
        check.add("gestation\t0.15038954324383452");
        check.add("immunesystem\t0.15038954324383452");
        check.add("insomnia\t0.15038954324383452");
        check.add("languagedisorder\t0.15038954324383452");
        check.add("medicalcare\t0.15038954324383452");
        check.add("medicalconditions\t0.15038954324383452");
        check.add("mentallydisabled\t0.15038954324383452");
        check.add("middle-of-the-nightinsomnia\t0.15038954324383452");
        check.add("mirrorneuronsystem\t0.15038954324383452");
        check.add("mutation\t0.15038954324383452");
        check.add("nervoussystem\t0.15038954324383452");
        check.add("netpresentvalue\t0.15038954324383452");
        check.add("neurochemical\t0.15038954324383452");
        check.add("neuronalmigration\t0.15038954324383452");
        check.add("pedantic\t0.15038954324383452");
        check.add("pediatrician\t0.15038954324383452");
        check.add("pervasivedevelopmentaldisorder\t0.15038954324383452");
        check.add("phenylketonuria\t0.15038954324383452");
        check.add("poormuscletone\t0.15038954324383452");
        check.add("regressiveautism\t0.15038954324383452");
        check.add("review\t0.15038954324383452");
        check.add("schizophrenia\t0.15038954324383452");
        check.add("self-referential\t0.15038954324383452");
        check.add("serotonin\t0.15038954324383452");
        check.add("skinpicking\t0.15038954324383452");
        check.add("socialcognition\t0.15038954324383452");
        check.add("task-positivenetwork\t0.15038954324383452");
        check.add("theoryofmind\t0.15038954324383452");
        check.add("toewalking\t0.15038954324383452");
        check.add("workingmemory\t0.15038954324383452");
        check.add("HistoryofAlbania\t0.150355232156644");   
        check.add("DemographicsofAlbania\t0.1502941689382532"); 
        check.add("As_We_May_Think\t0.15024007475072668");
        check.add("AllSaints\t0.1502010002793813"); 
        check.add("PoliticsofAlbania\t0.15015241928614023");
        check.add("EconomyofAlbania\t0.1500823643410853"); 
        check.add("AccessibleComputing\t0.15000000000000002");
        check.add("AfghanistanCommunications\t0.15000000000000002");
        check.add("AfghanistanGeography\t0.15000000000000002");
        check.add("AfghanistanHistory\t0.15000000000000002");
        check.add("AfghanistanMilitary\t0.15000000000000002");
        check.add("AfghanistanPeople\t0.15000000000000002");
        check.add("AfghanistanTransnationalIssues\t0.15000000000000002");
        check.add("AfghanistanTransportations\t0.15000000000000002");
        check.add("AlbaniaEconomy\t0.15000000000000002");
        check.add("AlbaniaGovernment\t0.15000000000000002");
        check.add("AlbaniaHistory\t0.15000000000000002");
        check.add("AlbaniaPeople\t0.15000000000000002");
        check.add("AmoeboidTaxa\t0.15000000000000002");
        check.add("Anarchism\t0.15000000000000002");
        check.add("AsWeMayThink\t0.15000000000000002");
        check.add("AssistiveTechnology\t0.15000000000000002");
        check.add("Autism\t0.15000000000000002");


        checkOutput(conf, new Path(output.toString()+"15sorted"), check);
    }

    private void checkOutput(Configuration conf, Path output,
            Map<String, String> c) throws IOException
    {
        FileSystem fs = FileSystem.getLocal(conf);

        assertThat(fs.exists(output), is(true));

        Path results = new Path(output, PART_R_00000);
        assertThat(fs.exists(results), is(true));

        FSDataInputStream is = fs.open(results);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line = null;
        Map<String, String> r = new HashMap<String, String>();
        while ((line = br.readLine()) != null)
        {
            String[] v = line.split("\t");
            r.put(v[0], v[1]);
        }
        assertThat(r.size(), is(c.size()));
        assertThat(r, is(c));
    }
    
    private void checkOutput(Configuration conf, Path output,
            List<String> check) throws IOException
    {
        FileSystem fs = FileSystem.getLocal(conf);

        assertThat(fs.exists(output), is(true));

        Path results = new Path(output, PART_R_00000);
        assertThat(fs.exists(results), is(true));

        FSDataInputStream is = fs.open(results);
        BufferedReader br = new BufferedReader(new InputStreamReader(is,Charset.forName("UTF-8")));
        String line = null;
        List<String> r = new ArrayList<String>();
        int i=0;
        while ((line = br.readLine()) != null)
        {
            r.add(line.trim());
            assertThat(line.trim(),is(check.get(i)));
            i++;
        }
        assertThat(r.size(), is(check.size()));
        assertThat(r, is(check));
    }
}
