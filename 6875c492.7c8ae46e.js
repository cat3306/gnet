/*! For license information please see 6875c492.7c8ae46e.js.LICENSE.txt */
(window.webpackJsonp=window.webpackJsonp||[]).push([[28],{170:function(e,t,a){"use strict";a.r(t);a(225);var n=a(0),r=a.n(n),u=a(221),l=a(270),c=a(215);t.default=function(e){var t=e.metadata,a=e.items,n=t.allTagsPath,i=t.name,o=t.count;return r.a.createElement(u.a,{title:'Posts tagged "'+i+'"',description:'Blog | Tagged "'+i+'"'},r.a.createElement("div",{className:"container margin-vert--lg"},r.a.createElement("div",{className:"row"},r.a.createElement("div",{className:"col col--8 col--offset-2"},r.a.createElement("h1",null,o," ",function(e,t){return e>1?t+"s":t}(o,"post"),' tagged with "',i,'"'),r.a.createElement(c.a,{href:n},"View All Tags"),r.a.createElement("div",{className:"margin-vert--xl"},a.map((function(e){var t=e.content;return r.a.createElement(l.a,{key:t.metadata.permalink,frontMatter:t.frontMatter,metadata:t.metadata,truncated:!0},r.a.createElement(t,null))})))))))}},216:function(e,t,a){"use strict";a.d(t,"a",(function(){return s})),a.d(t,"b",(function(){return E}));var n=a(0),r=a.n(n);function u(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function l(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function c(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?l(Object(a),!0).forEach((function(t){u(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},u=Object.keys(e);for(n=0;n<u.length;n++)a=u[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var u=Object.getOwnPropertySymbols(e);for(n=0;n<u.length;n++)a=u[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var o=r.a.createContext({}),D=function(e){var t=r.a.useContext(o),a=t;return e&&(a="function"==typeof e?e(t):c({},t,{},e)),a},s=function(e){var t=D(e.components);return r.a.createElement(o.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return r.a.createElement(r.a.Fragment,{},t)}},d=Object(n.forwardRef)((function(e,t){var a=e.components,n=e.mdxType,u=e.originalType,l=e.parentName,o=i(e,["components","mdxType","originalType","parentName"]),s=D(a),d=n,E=s["".concat(l,".").concat(d)]||s[d]||m[d]||u;return a?r.a.createElement(E,c({ref:t},o,{components:a})):r.a.createElement(E,c({ref:t},o))}));function E(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var u=a.length,l=new Array(u);l[0]=d;var c={};for(var i in t)hasOwnProperty.call(t,i)&&(c[i]=t[i]);c.originalType=e,c.mdxType="string"==typeof e?e:n,l[1]=c;for(var o=2;o<u;o++)l[o]=a[o];return r.a.createElement.apply(null,l)}return r.a.createElement.apply(null,a)}d.displayName="MDXCreateElement"},220:function(e,t,a){"use strict";a.d(t,"a",(function(){return r})),a.d(t,"b",(function(){return u}));var n=a(214);function r(){var e=Object(n.a)().siteConfig,t=(void 0===e?{}:e).customFields.metadata.latest_highlight,a=Date.parse(t.date),r=new Date,u=Math.abs(r-a),l=Math.ceil(u/864e5),c=null;return"undefined"!=typeof window&&(c=new Date(parseInt(window.localStorage.getItem("highlightsViewedAt")||"0"))),l<30&&(!c||c<a)?t:null}function u(){"undefined"!=typeof window&&window.localStorage.setItem("highlightsViewedAt",(new Date).getTime())}},221:function(e,t,a){"use strict";a(228);var n=a(0),r=a.n(n),u=a(229),l=a(218),c=a(1),i=(a(230),a(215)),o=a(231),D=a(219),s=a.n(D),m=a(232),d=a.n(m),E=a(214),f=a(213),g=a.n(f),b=a(131),h=a.n(b),p=function(){return r.a.createElement("span",{className:g()(h.a.toggle,h.a.moon)})},v=function(){return r.a.createElement("span",{className:g()(h.a.toggle,h.a.sun)})},F=function(e){var t=Object(E.a)().isClient;return r.a.createElement(d.a,Object(c.a)({disabled:!t,icons:{checked:r.a.createElement(p,null),unchecked:r.a.createElement(v,null)}},e))},y=a(220),C=a(217),N=a(236),O=a(226),k=a(227),w=a(224),_=a(132),j=a.n(_);function T(e){var t=e.href,a=e.hideIcon,n=e.label,u=e.onClick,l=(e.position,e.right),o=e.to,D=function(e,t){var a={label:e};switch(e.toLowerCase()){case"chat":return a.hideText=1==t,a.icon="message-circle",a;case"community":return a.hideText=1==t,a.icon="users",a;case"github":return a.badge="3.1k",a.hideText=!1,a.icon="github",a;case"highlights":return Object(y.a)()&&(a.badge="new",a.badgeStyle="primary"),a.hideText=1==t,a.icon="gift",a;default:return a}}(n,l)||{},s=Object(C.a)(o);return r.a.createElement(i.a,Object(c.a)({className:g()("navbar__item navbar__link",D.className,{navbar__item__icon_only:D.hideText}),title:D.hideText?n:null,onClick:u},t?{target:"_blank",rel:"noopener noreferrer",href:t}:{activeClassName:"navbar__link--active",to:s}),!a&&D.icon&&r.a.createElement(r.a.Fragment,null,r.a.createElement("i",{className:"feather icon-"+D.icon})," ",D.iconLabel),!D.hideText&&D.label,D.badge&&r.a.createElement("span",{className:g()("badge","badge--"+(D.badgeStyle||"secondary"))},D.badge))}var A=function(){var e,t=Object(E.a)(),a=t.siteConfig.themeConfig,u=a.navbar,l=(u=void 0===u?{}:u).title,D=u.links,m=void 0===D?[]:D,d=u.hideOnScroll,f=void 0!==d&&d,b=a.disableDarkMode,h=void 0!==b&&b,p=(t.isClient,Object(n.useState)(!1)),v=p[0],y=p[1],C=Object(n.useState)(!1),_=C[0],A=C[1],M=Object(w.a)(),B=M.isDarkTheme,S=M.setLightTheme,x=M.setDarkTheme,P=Object(N.a)(f),H=P.navbarRef,L=P.isNavbarVisible,I=Object(k.a)(),R=I.logoLink,G=I.logoLinkProps,J=I.logoImageUrl,W=I.logoAlt;Object(O.a)(v);var U=Object(n.useCallback)((function(){y(!0)}),[y]),z=Object(n.useCallback)((function(){y(!1)}),[y]),V=Object(n.useCallback)((function(e){return e.target.checked?x():S()}),[S,x]);return r.a.createElement("nav",{ref:H,className:g()("navbar","navbar--light","navbar--fixed-top",(e={"navbar-sidebar--show":v},e[j.a.navbarHideable]=f,e[j.a.navbarHidden]=!L,e))},r.a.createElement("div",{className:"navbar__inner"},r.a.createElement("div",{className:"navbar__items"},r.a.createElement("div",{"aria-label":"Navigation bar toggle",className:"navbar__toggle",role:"button",tabIndex:0,onClick:U,onKeyDown:U},r.a.createElement("svg",{xmlns:"http://www.w3.org/2000/svg",width:"30",height:"30",viewBox:"0 0 30 30",role:"img",focusable:"false"},r.a.createElement("title",null,"Menu"),r.a.createElement("path",{stroke:"currentColor",strokeLinecap:"round",strokeMiterlimit:"10",strokeWidth:"2",d:"M4 7h22M4 15h22M4 23h22"}))),r.a.createElement(i.a,Object(c.a)({className:"navbar__brand",to:R},G),null!=J&&r.a.createElement(s.a,{className:"navbar__logo",src:J,alt:W}),null!=l&&r.a.createElement("strong",{className:_?j.a.hideLogoText:""},l)),m.filter((function(e){return"right"!==e.position})).map((function(e,t){return r.a.createElement(T,Object(c.a)({},e,{left:!0,key:t}))}))),r.a.createElement("div",{className:"navbar__items navbar__items--right"},m.filter((function(e){return"right"===e.position})).map((function(e,t){return r.a.createElement(T,Object(c.a)({},e,{right:!0,key:t}))})),!h&&r.a.createElement(F,{className:j.a.displayOnlyInLargeViewport,"aria-label":"Dark mode toggle",checked:B,onChange:V}),r.a.createElement(o.a,{handleSearchBarToggle:A,isSearchBarExpanded:_}))),r.a.createElement("div",{role:"presentation",className:"navbar-sidebar__backdrop",onClick:z}),r.a.createElement("div",{className:"navbar-sidebar"},r.a.createElement("div",{className:"navbar-sidebar__brand"},r.a.createElement(i.a,Object(c.a)({className:"navbar__brand",onClick:z,to:R},G),null!=J&&r.a.createElement(s.a,{className:"navbar__logo",src:J,alt:W}),null!=l&&r.a.createElement("strong",null,l)),!h&&v&&r.a.createElement(F,{"aria-label":"Dark mode toggle in sidebar",checked:B,onChange:V})),r.a.createElement("div",{className:"navbar-sidebar__items"},r.a.createElement("div",{className:"menu"},r.a.createElement("ul",{className:"menu__list"},m.map((function(e,t){return r.a.createElement("li",{className:"menu__list-item",key:t},r.a.createElement(T,Object(c.a)({className:"menu__link"},e,{hideIcon:!0,onClick:z})))})))))))},M=a(9);a(133);var B=function(e){var t,a=e.block,n=e.buttonClass,u=e.center,l=e.description,c=e.size,i=e.width;return r.a.createElement("div",{className:g()("mailing-list",(t={"mailing-list--block":a,"mailing-list--center":u},t["mailing-list--"+c]=c,t))},!1!==l&&r.a.createElement("div",{className:"mailing-list--description"},"The easiest way to stay up-to-date. One email on the 1st of every month. No spam, ever."),r.a.createElement("form",{action:"https://app.getvero.com/forms/a748ded7ce0da69e6042fa1e21042506",method:"post",className:"mailing-list--form"},r.a.createElement("input",{className:g()("input","input--"+c),name:"email",placeholder:"you@email.com",type:"email",style:{width:i}}),r.a.createElement("button",{className:g()("button","button--"+(n||"primary"),"button--"+c),type:"submit"},"Subscribe")))},S=a(134),x=a.n(S);function P(e){var t=e.to,a=e.href,n=e.label,u=Object(M.a)(e,["to","href","label"]),l=Object(C.a)(t);return r.a.createElement(i.a,Object(c.a)({className:"footer__link-item"},a?{target:"_blank",rel:"noopener noreferrer",href:a}:{to:l},u),n)}var H=function(e){var t=e.url,a=e.alt;return r.a.createElement(s.a,{className:"footer__logo",alt:a,src:t})};var L=function(){var e=Object(E.a)().siteConfig,t=(void 0===e?{}:e).themeConfig,a=(void 0===t?{}:t).footer,n=a||{},u=n.copyright,l=n.links,c=void 0===l?[]:l,i=n.logo,o=void 0===i?{}:i,D=Object(C.a)(o.src);return a?r.a.createElement("footer",{className:g()("footer",{"footer--dark":"dark"===a.style})},r.a.createElement("div",{className:"container"},c&&c.length>0&&r.a.createElement("div",{className:"row footer__links"},r.a.createElement("div",{className:"col col--5 footer__col"},r.a.createElement("div",{className:"margin-bottom--md"},r.a.createElement(s.a,{className:"navbar__logo",src:"/img/logo-light.svg",alt:"Gnet",width:"150",height:"auto"})),r.a.createElement("div",{className:"margin-bottom--md"},r.a.createElement(B,{description:!1,width:"150px"})),r.a.createElement("div",null,r.a.createElement("a",{href:"https://twitter.com/_andy_pan",target:"_blank"},r.a.createElement("i",{className:"feather icon-twitter",alt:"Gnet's Twitter"})),"\xa0\xa0\xa0\xa0",r.a.createElement("a",{href:"https://github.com/panjf2000/gnet",target:"_blank"},r.a.createElement("i",{className:"feather icon-github",alt:"Gnet's Github Repo"})),"\xa0\xa0\xa0\xa0",r.a.createElement("a",{href:"https://taohuawu.club/rss.xml",target:"_blank"},r.a.createElement("i",{className:"feather icon-rss",alt:"Gnet's RSS feed"})))),c.map((function(e,t){return r.a.createElement("div",{key:t,className:"col footer__col"},null!=e.title?r.a.createElement("h4",{className:"footer__title"},e.title):null,null!=e.items&&Array.isArray(e.items)&&e.items.length>0?r.a.createElement("ul",{className:"footer__items"},e.items.map((function(e,t){return e.html?r.a.createElement("li",{key:t,className:"footer__item",dangerouslySetInnerHTML:{__html:e.html}}):r.a.createElement("li",{key:e.href||e.to,className:"footer__item"},r.a.createElement(P,e))}))):null)}))),(o||u)&&r.a.createElement("div",{className:"text--center"},o&&o.src&&r.a.createElement("div",{className:"margin-bottom--sm"},o.href?r.a.createElement("a",{href:o.href,target:"_blank",rel:"noopener noreferrer",className:x.a.footerLogoLink},r.a.createElement(H,{alt:o.alt,url:D})):r.a.createElement(H,{alt:o.alt,url:D}),r.a.createElement("br",null),r.a.createElement("a",{href:"https://www.digitalocean.com/",target:"_blank",rel:"noopener noreferrer",className:x.a.footerLogoLink},r.a.createElement("img",{alt:"DigitalOcean",src:"https://opensource.nyc3.cdn.digitaloceanspaces.com/attribution/assets/PoweredByDO/DO_Powered_by_Badge_blue.svg",width:"201px"}))),u,r.a.createElement("br",null),r.a.createElement("small",null,r.a.createElement("a",{href:"https://github.com/panjf2000/gnet/security/policy"},"Security Policy"),"\xa0\u2022\xa0",r.a.createElement("a",{href:"https://github.com/panjf2000/gnet/blob/master/PRIVACY.md"},"Privacy Policy")),r.a.createElement("br",null),r.a.createElement("small",null,"Acknowledgement for Timber, Inc.")))):null},I=a(238),R=a(237),G=a(3);a(135);t.a=function(e){var t=Object(E.a)().siteConfig,a=void 0===t?{}:t,n=a.favicon,c=(a.tagline,a.title),i=a.themeConfig.image,o=a.url,D=e.children,s=e.title,m=e.noFooter,d=e.description,f=e.image,g=e.keywords,b=(e.permalink,e.version),h=s?s+" | "+c:c,p=f||i,v=o+Object(C.a)(p),F=Object(C.a)(n),y=Object(G.h)(),N=y?"https://gnet.host"+(y.pathname.endsWith("/")?y.pathname:y.pathname+"/"):null;return r.a.createElement(R.a,null,r.a.createElement(I.a,null,r.a.createElement(l.a,null,r.a.createElement("html",{lang:"en"}),r.a.createElement("meta",{httpEquiv:"x-ua-compatible",content:"ie=edge"}),h&&r.a.createElement("title",null,h),h&&r.a.createElement("meta",{property:"og:title",content:h}),n&&r.a.createElement("link",{rel:"shortcut icon",href:F}),d&&r.a.createElement("meta",{name:"description",content:d}),d&&r.a.createElement("meta",{property:"og:description",content:d}),b&&r.a.createElement("meta",{name:"docsearch:version",content:b}),g&&g.length&&r.a.createElement("meta",{name:"keywords",content:g.join(",")}),p&&r.a.createElement("meta",{property:"og:image",content:v}),p&&r.a.createElement("meta",{property:"twitter:image",content:v}),p&&r.a.createElement("meta",{name:"twitter:image:alt",content:"Image for "+h}),p&&r.a.createElement("meta",{name:"twitter:site",content:"@vectordotdev"}),p&&r.a.createElement("meta",{name:"twitter:creator",content:"@vectordotdev"}),N&&r.a.createElement("meta",{property:"og:url",content:N}),r.a.createElement("meta",{name:"twitter:card",content:"summary"}),N&&r.a.createElement("link",{rel:"canonical",href:N})),r.a.createElement(u.a,null),r.a.createElement(A,null),r.a.createElement("div",{className:"main-wrapper"},D),!m&&r.a.createElement(L,null)))}},223:function(e,t,a){"use strict";a.d(t,"a",(function(){return u}));a(79),a(243),a(222),a(78);var n=a(235),r=a.n(n);function u(e,t){var a=new r.a;return e.map((function(e){var n=e;return"string"==typeof e&&(n={label:e,permalink:"/"+t+"/tags/"+a.slug(e)}),function(e,t){if(e.enriched)return e;var a=e.label.split(": ",2),n=a[0],r=a[1],u="primary";switch(n){case"domain":u="blue";break;case"type":u="pink";break;default:u="primary"}return{category:n,count:e.count,enriched:!0,label:e.label,permalink:e.permalink,style:u,value:r}}(n)}))}},225:function(e,t,a){var n=a(24).f,r=Function.prototype,u=/^\s*function ([^ (]*)/;"name"in r||a(10)&&n(r,"name",{configurable:!0,get:function(){try{return(""+this).match(u)[1]}catch(e){return""}}})},234:function(e,t,a){"use strict";var n=a(0),r=a.n(n),u=a(215),l=a(213),c=a.n(l);t.a=function(e){var t=e.count,a=e.label,n=e.permalink,l=e.style,i=e.value,o=e.valueOnly;return r.a.createElement(u.a,{to:n+"/",className:c()("badge","badge--rounded","badge--"+l)},o?i:a,t&&r.a.createElement(r.a.Fragment,null," (",t,")"))}},235:function(e,t,a){var n=a(245);e.exports=c;var r=Object.hasOwnProperty,u=/\s/g,l=/[\u2000-\u206F\u2E00-\u2E7F\\'!"#$%&()*+,./:;<=>?@[\]^`{|}~\u2019]/g;function c(){if(!(this instanceof c))return new c;this.reset()}function i(e,t){return"string"!=typeof e?"":(t||(e=e.toLowerCase()),e.trim().replace(l,"").replace(n(),"").replace(u,"-"))}c.prototype.slug=function(e,t){for(var a=i(e,!0===t),n=a;r.call(this.occurrences,a);)this.occurrences[n]++,a=n+"-"+this.occurrences[n];return this.occurrences[a]=0,a},c.prototype.reset=function(){this.occurrences=Object.create(null)},c.slug=i},239:function(e,t,a){"use strict";a(225),a(222);var n=a(0),r=a.n(n),u=a(213),l=a.n(u),c=a(214);a(137);t.a=function(e){var t,a=e.bio,n=e.className,u=e.github,i=e.nameSuffix,o=e.rel,D=e.size,s=e.subTitle,m=e.vertical,d=Object(c.a)().siteConfig,E=(void 0===d?{}:d).customFields.metadata.team,f=E.find((function(e){return e.github==u}))||E.find((function(e){return"ben"==e.id}));return r.a.createElement("div",{className:l()("avatar",n,(t={},t["avatar--"+D]=D,t["avatar--vertical"]=m,t))},r.a.createElement("img",{className:l()("avatar__photo","avatar__photo--"+D),src:f.avatar}),r.a.createElement("div",{className:"avatar__intro"},r.a.createElement("div",{className:"avatar__name"},r.a.createElement("a",{href:f.github,target:"_blank",rel:o},f.name),i),s&&r.a.createElement("small",{className:"avatar__subtitle"},s),!s&&a&&r.a.createElement("small",{className:"avatar__subtitle",dangerouslySetInnerHTML:{__html:f.bio}})))}},240:function(e,t,a){"use strict";var n=a(1),r=a(0),u=a.n(r),l=(a(215),a(234)),c=a(213),i=a.n(c),o=a(223),D=a(138),s=a.n(D);t.a=function(e){var t,a=e.block,r=e.colorProfile,c=e.tags,D=e.valuesOnly,m=Object(o.a)(c,r);return u.a.createElement("span",{className:i()(s.a.tags,(t={},t[s.a.tagsBlock]=a,t))},m.map((function(e,t){return u.a.createElement(l.a,Object(n.a)({key:t,valueOnly:D},e))})))}},241:function(e,t,a){var n;!function(r){"use strict";var u,l,c,i=(u=/d{1,4}|m{1,4}|yy(?:yy)?|([HhMsTt])\1?|[LloSZWN]|"[^"]*"|'[^']*'/g,l=/\b(?:[PMCEA][SDP]T|(?:Pacific|Mountain|Central|Eastern|Atlantic) (?:Standard|Daylight|Prevailing) Time|(?:GMT|UTC)(?:[-+]\d{4})?)\b/g,c=/[^-+\dA-Z]/g,function(e,t,a,n){if(1!==arguments.length||"string"!==m(e)||/\d/.test(e)||(t=e,e=void 0),(e=e||new Date)instanceof Date||(e=new Date(e)),isNaN(e))throw TypeError("Invalid date");var r=(t=String(i.masks[t]||t||i.masks.default)).slice(0,4);"UTC:"!==r&&"GMT:"!==r||(t=t.slice(4),a=!0,"GMT:"===r&&(n=!0));var d=a?"getUTC":"get",E=e[d+"Date"](),f=e[d+"Day"](),g=e[d+"Month"](),b=e[d+"FullYear"](),h=e[d+"Hours"](),p=e[d+"Minutes"](),v=e[d+"Seconds"](),F=e[d+"Milliseconds"](),y=a?0:e.getTimezoneOffset(),C=D(e),N=s(e),O={d:E,dd:o(E),ddd:i.i18n.dayNames[f],dddd:i.i18n.dayNames[f+7],m:g+1,mm:o(g+1),mmm:i.i18n.monthNames[g],mmmm:i.i18n.monthNames[g+12],yy:String(b).slice(2),yyyy:b,h:h%12||12,hh:o(h%12||12),H:h,HH:o(h),M:p,MM:o(p),s:v,ss:o(v),l:o(F,3),L:o(Math.round(F/10)),t:h<12?i.i18n.timeNames[0]:i.i18n.timeNames[1],tt:h<12?i.i18n.timeNames[2]:i.i18n.timeNames[3],T:h<12?i.i18n.timeNames[4]:i.i18n.timeNames[5],TT:h<12?i.i18n.timeNames[6]:i.i18n.timeNames[7],Z:n?"GMT":a?"UTC":(String(e).match(l)||[""]).pop().replace(c,""),o:(y>0?"-":"+")+o(100*Math.floor(Math.abs(y)/60)+Math.abs(y)%60,4),S:["th","st","nd","rd"][E%10>3?0:(E%100-E%10!=10)*E%10],W:C,N:N};return t.replace(u,(function(e){return e in O?O[e]:e.slice(1,e.length-1)}))});function o(e,t){for(e=String(e),t=t||2;e.length<t;)e="0"+e;return e}function D(e){var t=new Date(e.getFullYear(),e.getMonth(),e.getDate());t.setDate(t.getDate()-(t.getDay()+6)%7+3);var a=new Date(t.getFullYear(),0,4);a.setDate(a.getDate()-(a.getDay()+6)%7+3);var n=t.getTimezoneOffset()-a.getTimezoneOffset();t.setHours(t.getHours()-n);var r=(t-a)/6048e5;return 1+Math.floor(r)}function s(e){var t=e.getDay();return 0===t&&(t=7),t}function m(e){return null===e?"null":void 0===e?"undefined":"object"!=typeof e?typeof e:Array.isArray(e)?"array":{}.toString.call(e).slice(8,-1).toLowerCase()}i.masks={default:"ddd mmm dd yyyy HH:MM:ss",shortDate:"m/d/yy",mediumDate:"mmm d, yyyy",longDate:"mmmm d, yyyy",fullDate:"dddd, mmmm d, yyyy",shortTime:"h:MM TT",mediumTime:"h:MM:ss TT",longTime:"h:MM:ss TT Z",isoDate:"yyyy-mm-dd",isoTime:"HH:MM:ss",isoDateTime:"yyyy-mm-dd'T'HH:MM:sso",isoUtcDateTime:"UTC:yyyy-mm-dd'T'HH:MM:ss'Z'",expiresHeaderFormat:"ddd, dd mmm yyyy HH:MM:ss Z"},i.i18n={dayNames:["Sun","Mon","Tue","Wed","Thu","Fri","Sat","Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],monthNames:["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","January","February","March","April","May","June","July","August","September","October","November","December"],timeNames:["a","p","am","pm","A","P","AM","PM"]},void 0===(n=function(){return i}.call(t,a,t,e))||(e.exports=n)}()},242:function(e,t,a){"use strict";(function(e){var n=a(1),r=(a(247),a(248),a(78),a(79),a(250),a(0)),u=a.n(r),l=a(251),c=a.n(l),i=a(264),o=a(52),D=a(213),s=a.n(D),m=a(259),d=a(252),E=a.n(d),f=a(214),g=a(224),b=a(136),h=a.n(b);(void 0!==e?e:window).Prism=o.a,a(253),a(254),a(255),a(256),a(257),a(258);var p=/{([\d,-]+)}/,v=/title=".*"/;t.a=function(e){var t=e.children,a=e.className,l=e.metastring,o=Object(f.a)().siteConfig.themeConfig.prism,D=void 0===o?{}:o,d=Object(r.useState)(!1),b=d[0],F=d[1],y=Object(r.useState)(!1),C=y[0],N=y[1];Object(r.useEffect)((function(){N(!0)}),[]);var O=Object(r.useRef)(null),k=Object(r.useRef)(null),w=[],_="",j=Object(g.a)().isDarkTheme,T=D.theme||m.a,A=D.darkTheme||T,M=j?A:T;if(l&&p.test(l)){var B=l.match(p)[1];w=E.a.parse(B).filter((function(e){return e>0}))}l&&v.test(l)&&(_=l.match(v)[0].split("title=")[1].replace(/"+/g,"")),Object(r.useEffect)((function(){var e;return k.current&&(e=new c.a(k.current,{target:function(){return O.current}})),function(){e&&e.destroy()}}),[k.current,O.current]);var S=a&&a.replace(/language-/,"");!S&&D.defaultLanguage&&(S=D.defaultLanguage);var x=function(){window.getSelection().empty(),F(!0),setTimeout((function(){return F(!1)}),2e3)};return u.a.createElement(i.a,Object(n.a)({},i.b,{key:C,theme:M,code:t.trim(),language:S}),(function(e){var t,a,r=e.className,l=e.style,c=e.tokens,i=e.getLineProps,o=e.getTokenProps;return u.a.createElement(u.a.Fragment,null,_&&u.a.createElement("div",{style:l,className:h.a.codeBlockTitle},_),u.a.createElement("div",{className:h.a.codeBlockContent},u.a.createElement("button",{ref:k,type:"button","aria-label":"Copy code to clipboard",className:s()(h.a.copyButton,(t={},t[h.a.copyButtonWithTitle]=_,t)),onClick:x},b?"Copied":"Copy"),u.a.createElement("pre",{className:s()(r,h.a.codeBlock,(a={},a[h.a.codeBlockWithTitle]=_,a))},u.a.createElement("div",{ref:O,className:h.a.codeBlockLines,style:l},c.map((function(e,t){1===e.length&&""===e[0].content&&(e[0].content="\n");var a=i({line:e,key:t});return w.includes(t+1)&&(a.className=a.className+" docusaurus-highlight-code-line"),u.a.createElement("div",Object(n.a)({key:t},a),e.map((function(e,t){return u.a.createElement("span",Object(n.a)({key:t},o({token:e,key:t})))})))}))))))}))}}).call(this,a(77))},244:function(e,t,a){"use strict";var n=a(1),r=a(0),u=a.n(r),l=a(215),c=a(242),i=a(233),o=a(139),D=a.n(o);t.a={code:function(e){var t=e.children;return"string"==typeof t?u.a.createElement(c.a,e):t},a:function(e){return/\.[^./]+$/.test(e.href)?u.a.createElement("a",e):u.a.createElement(l.a,e)},pre:function(e){return u.a.createElement("div",Object(n.a)({className:D.a.mdxCodeBlock},e))},h1:Object(i.a)("h1"),h2:Object(i.a)("h2"),h3:Object(i.a)("h3"),h4:Object(i.a)("h4"),h5:Object(i.a)("h5"),h6:Object(i.a)("h6")}},245:function(e,t){e.exports=function(){return/[\xA9\xAE\u203C\u2049\u2122\u2139\u2194-\u2199\u21A9\u21AA\u231A\u231B\u2328\u23CF\u23E9-\u23F3\u23F8-\u23FA\u24C2\u25AA\u25AB\u25B6\u25C0\u25FB-\u25FE\u2600-\u2604\u260E\u2611\u2614\u2615\u2618\u261D\u2620\u2622\u2623\u2626\u262A\u262E\u262F\u2638-\u263A\u2648-\u2653\u2660\u2663\u2665\u2666\u2668\u267B\u267F\u2692-\u2694\u2696\u2697\u2699\u269B\u269C\u26A0\u26A1\u26AA\u26AB\u26B0\u26B1\u26BD\u26BE\u26C4\u26C5\u26C8\u26CE\u26CF\u26D1\u26D3\u26D4\u26E9\u26EA\u26F0-\u26F5\u26F7-\u26FA\u26FD\u2702\u2705\u2708-\u270D\u270F\u2712\u2714\u2716\u271D\u2721\u2728\u2733\u2734\u2744\u2747\u274C\u274E\u2753-\u2755\u2757\u2763\u2764\u2795-\u2797\u27A1\u27B0\u27BF\u2934\u2935\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55\u3030\u303D\u3297\u3299]|\uD83C[\uDC04\uDCCF\uDD70\uDD71\uDD7E\uDD7F\uDD8E\uDD91-\uDD9A\uDE01\uDE02\uDE1A\uDE2F\uDE32-\uDE3A\uDE50\uDE51\uDF00-\uDF21\uDF24-\uDF93\uDF96\uDF97\uDF99-\uDF9B\uDF9E-\uDFF0\uDFF3-\uDFF5\uDFF7-\uDFFF]|\uD83D[\uDC00-\uDCFD\uDCFF-\uDD3D\uDD49-\uDD4E\uDD50-\uDD67\uDD6F\uDD70\uDD73-\uDD79\uDD87\uDD8A-\uDD8D\uDD90\uDD95\uDD96\uDDA5\uDDA8\uDDB1\uDDB2\uDDBC\uDDC2-\uDDC4\uDDD1-\uDDD3\uDDDC-\uDDDE\uDDE1\uDDE3\uDDEF\uDDF3\uDDFA-\uDE4F\uDE80-\uDEC5\uDECB-\uDED0\uDEE0-\uDEE5\uDEE9\uDEEB\uDEEC\uDEF0\uDEF3]|\uD83E[\uDD10-\uDD18\uDD80-\uDD84\uDDC0]|\uD83C\uDDFF\uD83C[\uDDE6\uDDF2\uDDFC]|\uD83C\uDDFE\uD83C[\uDDEA\uDDF9]|\uD83C\uDDFD\uD83C\uDDF0|\uD83C\uDDFC\uD83C[\uDDEB\uDDF8]|\uD83C\uDDFB\uD83C[\uDDE6\uDDE8\uDDEA\uDDEC\uDDEE\uDDF3\uDDFA]|\uD83C\uDDFA\uD83C[\uDDE6\uDDEC\uDDF2\uDDF8\uDDFE\uDDFF]|\uD83C\uDDF9\uD83C[\uDDE6\uDDE8\uDDE9\uDDEB-\uDDED\uDDEF-\uDDF4\uDDF7\uDDF9\uDDFB\uDDFC\uDDFF]|\uD83C\uDDF8\uD83C[\uDDE6-\uDDEA\uDDEC-\uDDF4\uDDF7-\uDDF9\uDDFB\uDDFD-\uDDFF]|\uD83C\uDDF7\uD83C[\uDDEA\uDDF4\uDDF8\uDDFA\uDDFC]|\uD83C\uDDF6\uD83C\uDDE6|\uD83C\uDDF5\uD83C[\uDDE6\uDDEA-\uDDED\uDDF0-\uDDF3\uDDF7-\uDDF9\uDDFC\uDDFE]|\uD83C\uDDF4\uD83C\uDDF2|\uD83C\uDDF3\uD83C[\uDDE6\uDDE8\uDDEA-\uDDEC\uDDEE\uDDF1\uDDF4\uDDF5\uDDF7\uDDFA\uDDFF]|\uD83C\uDDF2\uD83C[\uDDE6\uDDE8-\uDDED\uDDF0-\uDDFF]|\uD83C\uDDF1\uD83C[\uDDE6-\uDDE8\uDDEE\uDDF0\uDDF7-\uDDFB\uDDFE]|\uD83C\uDDF0\uD83C[\uDDEA\uDDEC-\uDDEE\uDDF2\uDDF3\uDDF5\uDDF7\uDDFC\uDDFE\uDDFF]|\uD83C\uDDEF\uD83C[\uDDEA\uDDF2\uDDF4\uDDF5]|\uD83C\uDDEE\uD83C[\uDDE8-\uDDEA\uDDF1-\uDDF4\uDDF6-\uDDF9]|\uD83C\uDDED\uD83C[\uDDF0\uDDF2\uDDF3\uDDF7\uDDF9\uDDFA]|\uD83C\uDDEC\uD83C[\uDDE6\uDDE7\uDDE9-\uDDEE\uDDF1-\uDDF3\uDDF5-\uDDFA\uDDFC\uDDFE]|\uD83C\uDDEB\uD83C[\uDDEE-\uDDF0\uDDF2\uDDF4\uDDF7]|\uD83C\uDDEA\uD83C[\uDDE6\uDDE8\uDDEA\uDDEC\uDDED\uDDF7-\uDDFA]|\uD83C\uDDE9\uD83C[\uDDEA\uDDEC\uDDEF\uDDF0\uDDF2\uDDF4\uDDFF]|\uD83C\uDDE8\uD83C[\uDDE6\uDDE8\uDDE9\uDDEB-\uDDEE\uDDF0-\uDDF5\uDDF7\uDDFA-\uDDFF]|\uD83C\uDDE7\uD83C[\uDDE6\uDDE7\uDDE9-\uDDEF\uDDF1-\uDDF4\uDDF6-\uDDF9\uDDFB\uDDFC\uDDFE\uDDFF]|\uD83C\uDDE6\uD83C[\uDDE8-\uDDEC\uDDEE\uDDF1\uDDF2\uDDF4\uDDF6-\uDDFA\uDDFC\uDDFD\uDDFF]|[#\*0-9]\u20E3/g}},260:function(e,t,a){"use strict";a(271);var n=a(8),r=a(80),u=a(10),l=/./.toString,c=function(e){a(15)(RegExp.prototype,"toString",e,!0)};a(13)((function(){return"/a/b"!=l.call({source:"a",flags:"b"})}))?c((function(){var e=n(this);return"/".concat(e.source,"/","flags"in e?e.flags:!u&&e instanceof RegExp?r.call(e):void 0)})):"toString"!=l.name&&c((function(){return l.call(this)}))},261:function(e,t,a){e.exports=a(272)},270:function(e,t,a){"use strict";a(222),a(260),a(19);var n=a(0),r=a.n(n),u=a(239),l=a(215),c=(a(244),a(216),a(240)),i=a(213),o=a.n(i),D=a(241),s=a.n(D),m=a(223),d=a(261),E=a.n(d);t.a=function(e){var t=e.children,a=e.frontMatter,n=e.metadata,i=(e.truncated,e.isBlogPostPage,n.date),D=n.description,d=n.permalink,f=n.tags,g=a.author_github,b=a.title,h=E()(t.toString()),p=new Date(Date.parse(i)),v=Object(m.a)(f,"blog").find((function(e){return"domain"==e.category})),F=v?v.value:null;return r.a.createElement(l.a,{to:d+"/",className:o()("panel","domain-bg","domain-bg--hover","domain-bg--"+F)},r.a.createElement("article",null,r.a.createElement("h2",null,b),r.a.createElement("div",{className:"subtitle"},D),r.a.createElement(u.a,{github:g,size:"sm",subTitle:r.a.createElement(r.a.Fragment,null,r.a.createElement("time",{pubdate:"pubdate",dateTime:p.toISOString()},s()(p,"mmm dS"))," / ",h.text),rel:"author"}),r.a.createElement(c.a,{colorProfile:"blog",tags:f})))}},271:function(e,t,a){a(10)&&"g"!=/./g.flags&&a(24).f(RegExp.prototype,"flags",{configurable:!0,get:a(80)})},272:function(e,t,a){"use strict";function n(e){return" "===e||"\n"===e||"\r"===e||"\t"===e}e.exports=function(e,t){var a,r,u=0,l=0,c=e.length-1;for((t=t||{}).wordsPerMinute=t.wordsPerMinute||200,a=t.wordBound||n;a(e[l]);)l++;for(;a(e[c]);)c--;for(r=l;r<=c;){for(;r<=c&&!a(e[r]);r++);for(u++;r<=c&&a(e[r]);r++);}var i=u/t.wordsPerMinute,o=60*i*1e3;return{text:Math.ceil(i.toFixed(2))+" min read",minutes:i,time:o,words:u}}}}]);