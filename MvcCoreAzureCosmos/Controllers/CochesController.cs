using Microsoft.AspNetCore.Mvc;
using MvcCoreAzureCosmos.Models;
using MvcCoreAzureCosmos.Services;

namespace MvcCoreAzureCosmos.Controllers
{
    public class CochesController : Controller
    {
        private ServiceCosmosDb service;
        public CochesController(ServiceCosmosDb service)
        {
            this.service = service;
        }

        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Index(string accion)
        {
            await this.service.CreateDatabaseAsync();
            ViewData["MENSAJE"] = "Database Cosmos Creada!!!!";
            return View();
        }

        public async Task<IActionResult> MisCoches()
        {
            List<Coche> cars = await this.service.GetCochesAsync();
            return View(cars);
        }

        public IActionResult Create()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Create(Coche car, string existemotor)
        {
            if (existemotor == null)
            {
                car.Motor = null;
            }
            await this.service.InsertCocheAsync(car);
            return RedirectToAction("MisCoches");
        }

        public async Task<IActionResult> Delete(string id)
        {
            await this.service.DeleteCocheAsync(id);
            return RedirectToAction("MisCoches");
        }

        public async Task<IActionResult> Details(string id)
        {
            Coche car = await this.service.FindCocheAsync(id);
            return View(car);
        }

        public async Task<IActionResult> Edit(string id)
        {
            Coche car = await this.service.FindCocheAsync(id);
            return View(car);
        }

        [HttpPost]
        public async Task<IActionResult> Edit(Coche car)
        {
            await this.service.UpdateCocheAsync(car);
            return RedirectToAction("MisCoches");
        }
    }
}
