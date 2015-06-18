package my.springHive.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class test {

	//for web url
	@RequestMapping(value = "/test",
			        produces = "application/json;charset=UTF-8",  
			        method = RequestMethod.GET)
	public String printWelcome(ModelMap model) {

		//model.addAttribute("message", "Spring 3 MVC Hello World");
		return "index";
	}	
}
