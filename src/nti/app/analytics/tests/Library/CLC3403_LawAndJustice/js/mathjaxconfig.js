/**
 * $Revision: 10215 $
 **/
//config MathJax
MathJax.Hub.Config({
	showProcessingMessages : true,
	messageStyle : "none",
	"HTML-CSS": {
		 preferredFont: "STIX",
		 availableFonts: ["STIX"],
		 webFont: null,
		 imageFont: null
	},
	TeX : {
		Macros : {
			rlin : [ '\\overleftrightarrow{#1}', 1 ],
			vv : [ '\\overrightarrow{#1}', 1 ],
			Def: [ '\\textbf{#1}', 1],
			lcm: '\\text{lcm}',
			rule:['', 2],
			bm: ['\\boldsymbol{#1}', 1],
		    ensuremath: ['{#1}', 1],
			textsuperscript: ['^{\\text{#1}}', 1]
		}
	}
});

MathJax.Hub.Register.StartupHook("TeX Jax Ready",function () {
									 var TEX = MathJax.InputJax.TeX;
									 var MML = MathJax.ElementJax.mml;

									 TEX.Definitions.macros.Cube = "cube";
									 TEX.Definitions.macros.hdotsfor = "hdotsfor";
									 TEX.Definitions.macros.text = 'myText';
									 TEX.Definitions.macros.cancel = ["cancel",MML.NOTATION.UPDIAGONALSTRIKE];
									 TEX.Definitions.macros.bcancel = ["cancel",MML.NOTATION.DOWNDIAGONALSTRIKE];
									 TEX.Definitions.macros.nst = ['nsuperscript', 'st'];
									 TEX.Definitions.macros.nnd = ['nsuperscript', 'nd'];
									 TEX.Definitions.macros.nrd = ['nsuperscript', 'rd'];
									 TEX.Definitions.macros.nth = ['nsuperscript', 'th'];


									 //FIXME These aren't really math operators.  Maybe they need to be
									 //in mathchar0mi
									 TEX.Definitions.mathchar0mo.iddots = '22F0';
									 TEX.Definitions.mathchar0mo.yen = '00A5';
									 TEX.Definitions.mathchar0mo.textregistered = '00AE';
									 TEX.Definitions.mathchar0mo.mathsterling = '00A3';
									 TEX.Definitions.mathchar0mo.pounds = '00A3';
									 TEX.Definitions.mathchar0mo.EUR = '20AC';
									 TEX.Definitions.mathchar0mo.EURO = '20AC';
									 TEX.Definitions.mathchar0mo.euro = '20AC';
									 TEX.Definitions.mathchar0mo.smiley = '263A';
									 TEX.Definitions.mathchar0mo.cent = '00A2';


									 TEX.Parse.Augment({

										 nsuperscript: function(name, superscript){
											 var optionalprefix = this.GetArgument(name);
											 if (optionalprefix == null){
												 optionalprefix = "";
											 }
											 var rewritten = optionalprefix +'\\textsuperscript{'+superscript+'}';

											 var old = new RegExp('\\'+name+'\\s*{.*?}');

											 this.string = this.string.replace(old, rewritten);
											 this.i = this.string.indexOf(rewritten);

										 },

										 cancel: function (name,notation) {
											 var mml = this.ParseArg(name);
											 this.Push(MML.menclose(mml).With({notation:notation}));
										 },

										 cube: function (name) {
											 var arg = this.GetArgument(name);

											 var dice = ['\u2680', '\u2681', '\u2682', '\u2683', '\u2684', '\u2685'];

											 this.string = this.string.replace(/\\Cube\s*{.*?}/,dice[arg-1]);
											 this.i=this.string.indexOf(dice[arg-1]);

										 },

										 //FIXME This is close but not exact.  It's better than nothing?
										 hdotsfor: function(name){
											 var spacing = this.GetBrackets(name);
											 var cols = this.GetArgument(name);

											 var dots='';

											 for(var i=0;i<cols;i++){
												 dots+=' ... ';
												 if(i!=cols-1){
													 dots+='&';
												 }
											 }
											 this.string=this.string.replace(/\\hdotsfor\s*\[.*?\]*{.*?}/,dots);
											 this.i=this.string.indexOf(dots);
										 },

										 //FIXME This is also close but not exact.  Things that look like macros inside
										 //of text get reparsed and expanded.  The result is styled like math (italics).
									     //In some cases you can't tell, but in others the italics are noticible.  We
										 //get into some spacing issues when there are multiple words inside reexpanded text nodes.
										 myText: function(name){
											 var style=0;
											 var nodes=this.InternalMath(this.GetArgument(name), style);
											 var def = {displaystyle: false};
											 if (style != null)
											 {
												 def.scriptlevel = style;
											 }

											 var node=null;
											 for(var i=0;i<nodes.length;i++)
											 {
												 node=nodes[i];
												 if( node.__proto__.type === 'mtext'){

													 var text = node.data[0].toString();

													 if(text.search(/\\[^\s]+\s*(\[.*?\])*\s*({.*?})*/)>=0)
													 {
														var parseResult = TEX.Parse(node.data[0].data[0]).mml().With(def);
														nodes[i]=MML.TeXAtom(parseResult);
													 }
												 }
											 }
											 this.Push.apply(this, nodes);
										 }

									});
});

/*function textFromMJMML(MJMML)
{
	console.log('constructing text');
	console.log(MJMML);
	if(MJMML.__proto__.type === 'mo')
	{
		return MJMML.data[0].data[0].toString();
	}
	if(MJMML.__proto__.type === 'texatom')
	{
		return textFromMJMML(data[0]);
	}
	if(MJMML.__proto__.type === 'mrow')
	{
		var text='';
		for(var i=0;i<MJMML.data.length;i++)
		{
			text+=textFromMJMML(MJMML.data[i]);
		}
		return text;
	}

	return MJMML.toString();

}*/
