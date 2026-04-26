var out=[];
document.querySelectorAll(".wpProQuiz_question_page").forEach(function(qPage){
  var qNum=(qPage.innerText.match(/Question (\d+) of (\d+)/)||[])[0]||"";
  var items=[];
  qPage.querySelectorAll(".wpProQuiz_questionListItem").forEach(function(el){
    var cls=el.className;
    var txt=el.innerText.trim().substring(0,300);
    var tag="";
    if(cls.indexOf("answerCorrect")!==-1) tag="[CORRECT]";
    if(cls.indexOf("answerIncorrect")!==-1) tag="[WRONG]";
    if(tag) items.push(tag+" "+txt);
  });
  var explanation="";
  var expEl=qPage.querySelector(".wpProQuiz_response,.wpProQuiz_correct");
  if(expEl) explanation=expEl.innerText.trim().substring(0,500);
  if(items.length>0) out.push("--- "+qNum+"\n"+items.join("\n")+"\n"+(explanation?"EXPLANATION: "+explanation:""));
});
copy(out.join("\n\n"));
console.log("Copied "+out.length+" questions");
